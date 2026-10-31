'''
Copyright (C) 2018-2026 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.


Runs a Config: expands symbol patterns, splits the subscriptions into workers, runs one cryptofeed
FeedHandler per worker on its own thread and event loop, and hosts the stream servers, the stats
report and the health file on the main loop.

Workers are threads. On the free-threaded interpreter (3.15t) they run in parallel on separate
cores; on a GIL build they take turns on one core, so there a heavy load is better split across
containers. Each worker owns its backend writers (a BackendQueue belongs to one event loop); the
stream servers are shared and accept frames from any thread.
'''
import asyncio
import json
import logging
import os
import signal
import sys
import sysconfig
import threading
import time
from contextlib import suppress
from logging.handlers import RotatingFileHandler

from cryptofeed import FeedHandler
from cryptofeed.connection import connection_stats
from cryptofeed.exchanges import EXCHANGE_MAP

from cryptostore import __version__
from cryptostore.backends import BackendBuilder
from cryptostore.config import Config, ConfigError
from cryptostore.plan import plan_workers, resolve_symbols


LOG = logging.getLogger('cryptostore')
FORMAT = logging.Formatter('%(asctime)-15s : %(levelname)s : %(name)s : %(message)s')
STOP_SIGNALS = tuple(getattr(signal, name) for name in ('SIGINT', 'SIGTERM', 'SIGHUP') if hasattr(signal, name))
SHUTDOWN_GRACE = 30.0   # seconds for the workers to flush and finish after a stop request, before they are cancelled


def free_threading() -> tuple:
    '''(interpreter is a free-threaded build, the GIL is currently enabled)'''
    return bool(sysconfig.get_config_var('Py_GIL_DISABLED')), sys._is_gil_enabled()


def configure_logging(level: str = 'INFO', file: str = None, stream=None):
    root = logging.getLogger()
    root.setLevel(level.upper())
    for handler in list(root.handlers):
        if getattr(handler, '_cryptostore', False):
            root.removeHandler(handler)

    sh = logging.StreamHandler(stream or sys.stdout)
    sh.setFormatter(FORMAT)
    sh._cryptostore = True
    root.addHandler(sh)
    if file:
        fh = RotatingFileHandler(file, maxBytes=50 * 1024 * 1024, backupCount=10)
        fh.setFormatter(FORMAT)
        fh._cryptostore = True
        root.addHandler(fh)
    # cryptofeed's own logger otherwise swallows everything above its level
    logging.getLogger('cryptofeed').setLevel(level.upper())


class LoopLag:
    '''
    How far behind an event loop is running. A timer asks to sleep `interval` seconds; the lag is how
    much later than that it actually fired, i.e. the time the loop spent busy. A loop that cannot keep
    up with its feeds shows this climbing long before the exchange disconnects it for not reading.
    '''

    def __init__(self, interval: float = 1.0):
        self.interval = interval
        self.last = 0.0
        self.peak = 0.0      # since the last take_peak()

    async def run(self):
        loop = asyncio.get_running_loop()
        while True:
            started = loop.time()
            await asyncio.sleep(self.interval)
            lag = max(0.0, loop.time() - started - self.interval)
            self.last = lag
            if lag > self.peak:
                self.peak = lag

    def take_peak(self) -> float:
        peak, self.peak = self.peak, 0.0
        return peak

    def snapshot(self) -> dict:
        return {'loop_lag': round(self.last, 4), 'loop_lag_max': round(self.take_peak(), 4)}


class Worker:
    '''one FeedHandler on its own thread and event loop'''

    def __init__(self, plan, handler: FeedHandler, builder: BackendBuilder, loop_factory=None):
        self.plan = plan
        self.index = plan.index
        self.handler = handler
        self.builder = builder
        self.loop_factory = loop_factory
        self.labels = {}             # id(feed) -> shard label, e.g. BINANCE[2/8]
        self.lag = LoopLag()
        self.thread = None
        self.loop = None
        self.done = None             # future on the main loop, resolved once the thread has exited
        self.error = None
        self._run_task = None
        self._stop_requested = False

    @property
    def name(self) -> str:
        return f'worker{self.index}'

    def start(self, main_loop: asyncio.AbstractEventLoop):
        self.done = main_loop.create_future()
        self.thread = threading.Thread(target=self._thread_main, args=(main_loop,), name=f'cryptostore.{self.name}', daemon=True)
        self.thread.start()

    def _thread_main(self, main_loop):
        try:
            with asyncio.Runner(loop_factory=self.loop_factory) as runner:
                self.loop = runner.get_loop()
                runner.run(self._main())
        except BaseException as e:   # reported on the main loop, which decides what to do
            if not (isinstance(e, asyncio.CancelledError) and self._stop_requested):
                self.error = e
        finally:
            self.loop = None
            with suppress(RuntimeError):
                main_loop.call_soon_threadsafe(self._finished)

    def _finished(self):
        if not self.done.done():
            self.done.set_result(None)

    async def _main(self):
        if self._stop_requested:
            return
        lag_task = asyncio.create_task(self.lag.run(), name=f'{self.name}.lag')
        self._run_task = asyncio.create_task(self.handler.run_async(install_signal_handlers=False), name=f'{self.name}.feedhandler')
        try:
            await self._run_task
        finally:
            lag_task.cancel()
            await asyncio.gather(lag_task, return_exceptions=True)

    def request_stop(self):
        '''graceful: every feed flushes its backends and finishes. Safe from any thread, at any time'''
        self._stop_requested = True
        self._call(self._apply_stop)

    def cancel(self):
        '''immediate: cancels the handler, as a second signal does'''
        self._stop_requested = True
        self._call(self._apply_cancel)

    def _apply_stop(self):
        if self.handler.running:
            self.handler.request_stop()
        elif self._run_task is not None:
            self._run_task.cancel()    # has not started yet, so there is nothing to flush

    def _apply_cancel(self):
        if self._run_task is not None:
            self._run_task.cancel()

    def _call(self, fn):
        loop = self.loop
        if loop is not None:
            with suppress(RuntimeError):
                loop.call_soon_threadsafe(fn)

    def feeds(self) -> list:
        return [self.labels.get(id(feed), feed.id) for feed in list(self.handler.feeds)]

    def removed_feeds(self) -> list:
        return [self.labels.get(id(feed), feed.id) for feed in list(self.handler.removed_feeds)]

    def snapshot(self) -> dict:
        snap = {'feeds': self.feeds(), 'symbols': self.plan.symbols, 'alive': self.thread is not None and self.thread.is_alive()}
        snap.update(self.lag.snapshot())
        return snap


class Cryptostore:
    def __init__(self, config: Config):
        self.config = config
        self.resolved = None     # the config with symbol patterns expanded
        self.plan = None
        self.servers = {}        # stream servers, shared by every worker
        self.workers = []
        self.recorder = None
        self.lag = LoopLag()     # the main loop: stream fan-out and stats
        self._stats_task = None
        self._signals = 0

    @property
    def handlers(self) -> list:
        return [worker.handler for worker in self.workers]

    def cryptofeed_config(self) -> dict:
        cfg = dict(self.config.cryptofeed)
        cfg['log'] = {'disabled': True}   # cryptostore owns the handlers, cryptofeed propagates to them
        return cfg

    def _loop_factory(self):
        if self.config.cryptofeed.get('uvloop', True):
            try:
                import uvloop
                return uvloop.new_event_loop
            except ImportError:
                pass
        return None

    # ------------------------------------------------------------------ building

    def build(self, record: bool = True) -> list:
        '''
        Expands symbol patterns (downloading the symbol list of every exchange that uses one), plans the
        workers and constructs every feed. Returns the workers.

        record: False skips the pcap recorder, which creates its output directory when constructed.
        `cryptostore check` uses that so validating a config has no side effects
        '''
        if self.resolved is None:
            if self.config.exchanges_with_patterns():
                asyncio.run(self._resolve())
            else:
                self.resolved = self.config
        return self._construct(record)

    async def build_async(self, record: bool = True) -> list:
        if self.resolved is None:
            await self._resolve()
        return self._construct(record)

    async def _resolve(self):
        known = {}
        ttl = self.config.cryptofeed.get('symbol_cache_ttl') or None
        for exchange in self.config.exchanges:
            if not exchange.patterns:
                continue
            cls = EXCHANGE_MAP[exchange.name]
            LOG.info('%s: loading the symbol list to expand %s', exchange.name, ', '.join(exchange.patterns))
            try:
                await cls.load_symbols(cache_ttl=ttl)
            except Exception as e:
                raise ConfigError(f'{exchange.name}: could not load the symbol list needed to expand {", ".join(exchange.patterns)} ({type(e).__name__}: {e})') from None
            known[exchange.name] = cls.symbols()
        self.resolved = resolve_symbols(self.config, known)

    def _construct(self, record: bool) -> list:
        config = self.resolved
        recorder = None
        if record and config.record is not None:
            from cryptofeed.capture import PcapRecorder
            self.recorder = recorder = PcapRecorder(config.record.path, rotate_size=config.record.rotate_size, compress=config.record.compress)

        cf_config = self.cryptofeed_config()
        loop_factory = self._loop_factory()
        self.plan = plan_workers(config)
        self.workers = []
        for plan in self.plan:
            # writers are per worker (a queue belongs to one loop); the stream servers are shared
            builder = BackendBuilder(config.backends, servers=self.servers)
            handler = FeedHandler(config=cf_config, on_feed_error=config.on_feed_error, record=recorder)   # recording implies one worker
            worker = Worker(plan, handler, builder, loop_factory)
            for shard in plan.shards:
                exchange = shard.exchange
                callbacks = {channel.channel: [builder.writer(backend, channel.channel) for backend in channel.backends] for channel in exchange.channels}
                feed = EXCHANGE_MAP[exchange.name](subscription=exchange.subscription, callbacks=callbacks, config=cf_config, **exchange.options)
                handler.add_feed(feed)
                worker.labels[id(feed)] = shard.label
            self.workers.append(worker)
        return self.workers

    def describe(self) -> str:
        config = self.resolved or self.config
        free_threaded, gil_enabled = free_threading()
        interpreter = f'python {sys.version.split()[0]}' + (' free-threaded' if free_threaded else '') + (' with the GIL enabled' if free_threaded and gil_enabled else '')
        lines = [f'cryptostore {__version__} - {interpreter}']
        if self.plan is not None:
            requested = config.workers.count
            note = f' of {requested} configured - the rest would have nothing to do' if len(self.plan) != requested else ''
            lines.append(f'  {len(self.plan)} worker(s){note}')
            for plan in self.plan:
                lines.append(f'  worker {plan.index}: {plan.symbols} symbol(s)')
                for shard in plan.shards:
                    lines.extend(_describe_exchange(shard.exchange, shard.label, '    '))
        else:
            for exchange in config.exchanges:
                lines.extend(_describe_exchange(exchange, exchange.name, '  '))
        for name in config.backends_used():
            backend = config.backends[name]
            shown = {key: ('***' if key in ('pw', 'password', 'token') else value) for key, value in backend.options.items()}
            lines.append(f'  backend {name} ({backend.type.name}) {shown}')
        if config.record is not None:
            lines.append(f'  recording raw exchange traffic to {config.record.path}')
        return '\n'.join(lines)

    # ------------------------------------------------------------------ running

    async def run_async(self):
        if not self.workers:
            await self.build_async()
        loop = asyncio.get_running_loop()
        config = self.resolved

        LOG.info('starting\n%s', self.describe())
        free_threaded, gil_enabled = free_threading()
        if free_threaded and gil_enabled:
            LOG.warning('free-threaded interpreter, but the GIL is enabled: a C extension that does not declare free-threading support was imported '
                        '(hiredis is the usual one - run redis-py without it). PYTHON_GIL=0 overrides this at your own risk')
        if len(self.workers) > 1 and (not free_threaded or gil_enabled):
            LOG.warning('%d workers but the GIL is in effect, so the threads take turns on one core. Use the free-threaded image (the -t tag) '
                        'for parallelism, or split the config across containers', len(self.workers))

        for server in self.servers.values():
            await server.start()
        installed = self._install_signal_handlers(loop)
        lag_task = asyncio.create_task(self.lag.run(), name='cryptostore.lag')
        if config.stats_interval > 0:
            self._stats_task = asyncio.create_task(self._stats_loop(), name='cryptostore.stats')
        try:
            for worker in self.workers:
                worker.start(loop)
            LOG.info('%d worker thread(s) started', len(self.workers))
            await self._supervise()
        finally:
            for task in (lag_task, self._stats_task):
                if task is not None:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
            self._stats_task = None
            await self._stop_workers()
            self._remove_signal_handlers(loop, installed)
            for server in self.servers.values():
                await server.close()
            self.report(final=True)
            LOG.info('stopped')

    def run(self) -> int:
        '''blocking entry point. Returns the exit code'''
        with asyncio.Runner(loop_factory=self._loop_factory()) as runner:
            try:
                runner.run(self.run_async())
            except asyncio.CancelledError:
                LOG.warning('cancelled')
                return 130
            except ConfigError:
                raise
            except Exception:
                LOG.exception('stopped after a failure')
                return 1
        return 0

    async def _supervise(self):
        '''wait for the workers; a failed worker stops the others and its error is re-raised once all are done'''
        pending = {worker.done for worker in self.workers}
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for worker in self.workers:
                if worker.done not in done:
                    continue
                if worker.error is None:
                    LOG.info('%s finished', worker.name)
                else:
                    LOG.error('%s failed: %r', worker.name, worker.error)
                    if pending:
                        LOG.error('stopping the remaining workers')
                        self.request_stop()
        errors = [worker.error for worker in self.workers if worker.error is not None]
        if errors:
            raise errors[0]

    async def _stop_workers(self):
        self.request_stop()
        pending = [worker.done for worker in self.workers if worker.done is not None and not worker.done.done()]
        if pending:
            _, pending = await asyncio.wait(pending, timeout=SHUTDOWN_GRACE)
            if pending:
                LOG.warning('%d worker(s) still running after %.0fs - cancelling', len(pending), SHUTDOWN_GRACE)
                self.cancel()
                await asyncio.wait(pending, timeout=10)
        for worker in self.workers:
            if worker.thread is not None:
                await asyncio.to_thread(worker.thread.join, 5)

    def request_stop(self):
        '''ask every worker to finish gracefully. Safe from any thread'''
        for worker in self.workers:
            worker.request_stop()

    def cancel(self):
        for worker in self.workers:
            worker.cancel()

    def _install_signal_handlers(self, loop) -> list:
        if threading.current_thread() is not threading.main_thread():
            return []
        installed = []
        for sig in STOP_SIGNALS:
            try:
                loop.add_signal_handler(sig, self._on_signal, sig)
            except (NotImplementedError, RuntimeError, ValueError):
                continue
            installed.append(sig)
        return installed

    @staticmethod
    def _remove_signal_handlers(loop, installed: list):
        for sig in installed:
            with suppress(Exception):
                loop.remove_signal_handler(sig)

    def _on_signal(self, sig):
        self._signals += 1
        if self._signals == 1:
            LOG.info('%s received - stopping gracefully (send it again to cancel)', sig.name)
            self.request_stop()
        else:
            LOG.warning('%s received again - cancelling', sig.name)
            self.cancel()

    # ------------------------------------------------------------------ stats

    async def _stats_loop(self):
        while True:
            await asyncio.sleep(self.config.stats_interval)
            try:
                self.report()
            except Exception:
                LOG.exception('stats report failed')

    def snapshot(self) -> dict:
        '''everything the stats report and the health file contain'''
        config = self.resolved or self.config
        # per backend (the names from the config), per channel: write counters summed over the workers'
        # writers. Stream backends add their server's client counters
        backends = {}
        for worker in self.workers:
            for (name, channel), writer in worker.builder.writers().items():
                entry = backends.setdefault(name, {'type': config.backends[name].type.name, 'channels': {}})
                stats = getattr(writer, 'stats', None)   # tty has none
                if stats is not None:
                    _accumulate(entry['channels'], channel, _stats_dict(stats))
        for name, server in self.servers.items():
            backends.setdefault(name, {'type': 'stream', 'channels': {}}).update(
                clients=server.clients, connections=server.connections, published=server.published, kicked=server.kicked)

        free_threaded, gil_enabled = free_threading()
        return {
            'timestamp': time.time(),
            'version': __version__,
            'python': sys.version.split()[0],
            'free_threaded': free_threaded,
            'gil_enabled': gil_enabled,
            'feeds': [label for worker in self.workers for label in worker.feeds()],
            'removed_feeds': [label for worker in self.workers for label in worker.removed_feeds()],
            'workers': {str(worker.index): worker.snapshot() for worker in self.workers},
            'main_loop': self.lag.snapshot(),
            'backends': backends,
            'connections': _connection_stats(),
        }

    def report(self, final: bool = False):
        if not self.workers:
            return
        snap = self.snapshot()
        label = 'final stats' if final else 'stats'
        lag_warn = (self.resolved or self.config).workers.lag_warn

        for index, worker in snap['workers'].items():
            LOG.info('%s: worker %s feeds=%d symbols=%d loop_lag=%.3fs max=%.3fs', label, index, len(worker['feeds']), worker['symbols'], worker['loop_lag'], worker['loop_lag_max'])
            if worker['loop_lag_max'] > lag_warn:
                LOG.warning('%s: worker %s was up to %.1fs behind - its event loop cannot keep up with its feeds. Add workers (free-threaded python) '
                            'or split the config across containers', label, index, worker['loop_lag_max'])
        if snap['main_loop']['loop_lag_max'] > lag_warn:
            LOG.warning('%s: the main loop (stream fan-out) was up to %.1fs behind', label, snap['main_loop']['loop_lag_max'])
        for conn_id, stats in snap['connections'].items():
            LOG.info('%s: conn %s received=%d reconnects=%d watchdog=%d open=%s', label, conn_id, stats['received'], stats['reconnects'], stats['watchdog_trips'], stats['open'])
        for name, backend in snap['backends'].items():
            for channel, stats in backend['channels'].items():
                LOG.info('%s: backend %s %s written=%d qsize=%d dropped=%d retries=%d last_error=%s', label, name, channel, stats['written'], stats['qsize'],
                         stats['dropped_overflow'] + stats['dropped_failed'], stats['retries'], stats['last_error'])
            if 'clients' in backend:
                LOG.info('%s: backend %s clients=%d connections=%d published=%d kicked=%d', label, name, backend['clients'], backend['connections'], backend['published'], backend['kicked'])
        if snap['removed_feeds']:
            LOG.warning('%s: feeds removed after permanent failure: %s', label, ', '.join(snap['removed_feeds']))

        if self.config.health_file:
            write_health(self.config.health_file, snap)


def _describe_exchange(exchange, label: str, indent: str) -> list:
    options = f' options={exchange.options}' if exchange.options else ''
    lines = [f'{indent}{label}{options}']
    for channel in exchange.channels:
        source = f' from {", ".join(channel.patterns)}' if channel.patterns else ''
        lines.append(f'{indent}  {channel.channel:<14} {len(channel.symbols):>5} symbol(s){source} -> {", ".join(channel.backends)}')
    return lines


def _stats_dict(stats) -> dict:
    return {key: getattr(stats, key) for key in stats.__dataclass_fields__}


def _accumulate(channels: dict, channel: str, stats: dict):
    '''sum one writer's counters into the channel's entry - each worker has its own writer for the channel'''
    entry = channels.get(channel)
    if entry is None:
        channels[channel] = dict(stats)
        return
    for key, value in stats.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            if value is not None:
                entry[key] = value
        elif key.endswith('_ts'):
            entry[key] = max(entry.get(key) or 0, value)
        else:
            entry[key] = (entry.get(key) or 0) + value


def _connection_stats() -> dict:
    # the registry is a WeakSet that worker threads add to; an iteration can collide with that
    for _ in range(3):
        try:
            return {conn_id: _stats_dict(stats) for conn_id, stats in connection_stats().items()}
        except RuntimeError:
            continue
    return {}


def write_health(path: str, snap: dict):
    tmp = f'{path}.tmp'
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    with open(tmp, 'w') as fp:
        json.dump(snap, fp, default=str)
    os.replace(tmp, path)


def check_health(path: str, max_age: float) -> tuple:
    '''(healthy, message) for a health file written by the stats loop'''
    try:
        with open(path) as fp:
            snap = json.load(fp)
    except FileNotFoundError:
        return False, f'health file {path} does not exist yet'
    except (OSError, ValueError) as e:
        return False, f'health file {path} unreadable: {e}'

    age = time.time() - snap.get('timestamp', 0)
    if age > max_age:
        return False, f'health file {path} is {age:.0f}s old (limit {max_age:.0f}s)'
    if not snap.get('feeds'):
        return False, 'no feeds are running'
    return True, f'ok - {len(snap["feeds"])} feed(s), updated {age:.0f}s ago'
