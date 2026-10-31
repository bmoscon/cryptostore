'''
Copyright (C) 2018-2026 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import asyncio
import json
import logging
import time
from decimal import Decimal

import pytest
from cryptofeed import FeedHandler
from cryptofeed.backends.socket import BookSocket, TradeSocket
from cryptofeed.defines import BUY, L2_BOOK, TICKER, TRADES
from cryptofeed.symbols import Symbols
from cryptofeed.types import Trade

from cryptostore import cli
from cryptostore.backends import TTY
from cryptostore.config import ConfigError, parse
from cryptostore.runner import Cryptostore, LoopLag, check_health, configure_logging, write_health
from cryptostore.stream import TradeStream


def test_build_wires_feeds_and_shared_backends(base_config):
    base_config['backends']['sock'] = {'type': 'socket', 'addr': 'tcp://127.0.0.1', 'port': 9, 'queue': {'max_depth': 11}}
    base_config['exchanges']['KRAKEN'] = {'channels': {'trades': ['BTC-USD'], 'ticker': {'symbols': ['ETH-USD'], 'backends': ['out']}}, 'options': {'max_depth': 25}}
    config = parse(base_config)
    app = Cryptostore(config)
    (worker,) = app.build()
    handler = worker.handler

    assert isinstance(handler, FeedHandler) and app.handlers == [handler]
    assert handler.on_feed_error == 'raise'
    assert [feed.id for feed in handler.feeds] == ['COINBASE', 'KRAKEN']
    coinbase, kraken = handler.feeds

    assert coinbase.subscription == {}   # resolved later, once symbols are loaded
    assert coinbase._init_subscription == {TRADES: ['BTC-USD', 'ETH-USD'], L2_BOOK: ['BTC-USD']}
    assert kraken._init_subscription == {TRADES: ['BTC-USD'], TICKER: ['ETH-USD']}
    assert kraken.max_depth == 25

    # one socket backend instance per channel, shared by both exchanges
    cb_trades = [cb for cb in coinbase.callbacks[TRADES] if isinstance(cb, TradeSocket)]
    kr_trades = [cb for cb in kraken.callbacks[TRADES] if isinstance(cb, TradeSocket)]
    assert len(cb_trades) == len(kr_trades) == 1 and cb_trades[0] is kr_trades[0]
    assert cb_trades[0].max_depth == 11
    assert any(isinstance(cb, BookSocket) for cb in coinbase.callbacks[L2_BOOK])
    assert any(isinstance(cb, TTY) for cb in coinbase.callbacks[TRADES])
    # the ticker channel was routed only to the tty backend
    assert [type(cb) for cb in kraken.callbacks[TICKER]] == [TTY]

    # cryptofeed's logging is handed to cryptostore's handlers
    assert handler.config.log.disabled is True
    assert coinbase.config.log.disabled is True
    description = app.describe()
    assert 'COINBASE' in description and 'KRAKEN options=' in description and '-> out, sock' in description


def test_check_does_not_create_the_capture_directory(base_config, tmp_path):
    base_config['record'] = str(tmp_path / 'captures') + '/'
    app = Cryptostore(parse(base_config))
    (worker,) = app.build(record=False)
    assert worker.handler.recorder is None and app.recorder is None
    assert not (tmp_path / 'captures').exists()


def test_build_with_record_and_cryptofeed_passthrough(base_config, tmp_path):
    base_config['record'] = {'path': str(tmp_path / 'captures') + '/', 'rotate_size': 4096}
    base_config['cryptofeed'] = {'symbol_cache_ttl': 1234, 'uvloop': False}
    base_config['on_feed_error'] = 'remove_feed'
    app = Cryptostore(parse(base_config))
    (worker,) = app.build()
    handler = worker.handler
    assert handler.on_feed_error == 'remove_feed'
    assert handler.recorder is app.recorder and app.recorder is not None
    assert app.recorder.rotate_size == 4096
    assert handler.config.symbol_cache_ttl == 1234
    assert handler.feeds[0].config.symbol_cache_ttl == 1234
    # free the module-level recorder hook for the next test
    from cryptofeed.connection import Connection
    Connection.raw_data_callback = None
    asyncio.run(app.recorder.aclose())


async def test_run_async_starts_servers_and_stops_cleanly(base_config, tmp_path):
    health = tmp_path / 'health.json'
    base_config['backends']['stream'] = {'type': 'stream', 'tcp': '127.0.0.1:0'}
    base_config['stats_interval'] = 0.05
    base_config['health_file'] = str(health)
    base_config['cryptofeed'] = {'uvloop': False}
    app = Cryptostore(parse(base_config))
    (worker,) = app.build()
    handler = worker.handler

    # never connect to Coinbase: replace the feed's run with one that idles until asked to stop
    async def idle(stop_event):
        await stop_event.wait()
    handler.feeds[0].run = idle

    async def stop_soon():
        await asyncio.sleep(0.3)
        assert app.servers['stream'].tcp_port, 'the stream server should be listening while feeds run'
        assert worker.thread.is_alive() and worker.thread is not __import__('threading').current_thread()
        app.request_stop()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(stop_soon())
        await app.run_async()

    assert health.exists()
    snap = json.loads(health.read_text())
    assert snap['feeds'] == ['COINBASE']
    assert snap['free_threaded'] is bool(__import__('sysconfig').get_config_var('Py_GIL_DISABLED')) and isinstance(snap['gil_enabled'], bool)
    assert set(snap) >= {'backends', 'connections'} and 'streams' not in snap
    stream = snap['backends']['stream']
    assert stream['type'] == 'stream' and stream['clients'] == 0 and stream['published'] == 0
    assert stream['channels']['trades']['written'] == 0 and stream['channels']['trades']['dropped_overflow'] == 0
    assert snap['workers']['0']['feeds'] == ['COINBASE'] and snap['workers']['0']['symbols'] == 3
    assert snap['workers']['0']['loop_lag'] >= 0 and snap['workers']['0']['loop_lag_max'] >= 0 and snap['main_loop']['loop_lag'] >= 0
    assert any(isinstance(cb, TradeStream) for cb in handler.feeds[0].callbacks[TRADES])
    assert app.servers['stream']._servers == []   # closed
    assert not worker.thread.is_alive() and worker.error is None

    healthy, message = check_health(str(health), max_age=60)
    assert healthy, message


def test_check_health_states(tmp_path):
    path = tmp_path / 'h.json'
    healthy, message = check_health(str(path), 10)
    assert not healthy and 'does not exist' in message

    write_health(str(path), {'timestamp': time.time() - 100, 'feeds': ['X']})
    healthy, message = check_health(str(path), 10)
    assert not healthy and 'old' in message

    write_health(str(path), {'timestamp': time.time(), 'feeds': []})
    assert check_health(str(path), 10) == (False, 'no feeds are running')

    write_health(str(path), {'timestamp': time.time(), 'feeds': ['X']})
    assert check_health(str(path), 10)[0]
    assert not (tmp_path / 'h.json.tmp').exists()


def test_configure_logging_routes_cryptofeed_to_root(capsys):
    configure_logging('DEBUG')
    logging.getLogger('cryptofeed.feedhandler').debug('hello from cryptofeed')
    logging.getLogger('cryptostore').info('hello from cryptostore')
    out = capsys.readouterr().out
    assert 'hello from cryptofeed' in out and 'hello from cryptostore' in out
    # a second call replaces, not duplicates, the handlers
    configure_logging('INFO')
    assert sum(getattr(h, '_cryptostore', False) for h in logging.getLogger().handlers) == 1


def test_cli_check_and_listing(tmp_path, capsys):
    path = tmp_path / 'config.yaml'
    path.write_text('''
backends:
  out: {type: tty}
exchanges:
  COINBASE:
    channels: {trades: [BTC-USD]}
''')
    assert cli.main(['check', str(path)]) == 0
    out = capsys.readouterr().out
    assert 'ok' in out and 'COINBASE' in out

    assert cli.main(['exchanges']) == 0
    assert 'COINBASE' in capsys.readouterr().out
    assert cli.main(['exchanges', 'binance']) == 0
    out = capsys.readouterr().out
    assert 'depth_interval' in out and 'l2_book' in out
    assert cli.main(['backends']) == 0
    assert 'postgres' in capsys.readouterr().out


def test_cli_reports_config_errors(tmp_path, capsys):
    path = tmp_path / 'bad.yaml'
    path.write_text('backends: {out: {type: tty}}\nexchanges: {COINBSE: {channels: {trades: [BTC-USD]}}}\n')
    assert cli.main(['check', str(path)]) == 2
    assert "did you mean 'COINBASE'" in capsys.readouterr().err
    assert cli.main(['run', str(tmp_path / 'missing.yaml')]) == 2
    assert 'not found' in capsys.readouterr().err


def test_cli_health(tmp_path, capsys):
    health = tmp_path / 'health.json'
    path = tmp_path / 'config.yaml'
    path.write_text(f'''
backends: {{out: {{type: tty}}}}
exchanges: {{COINBASE: {{channels: {{trades: [BTC-USD]}}}}}}
health_file: {health}
stats_interval: 5
''')
    assert cli.main(['health', str(path)]) == 1
    write_health(str(health), {'timestamp': time.time(), 'feeds': ['COINBASE']})
    assert cli.main(['health', str(path)]) == 0
    assert cli.main(['health', str(path), '--max-age', '0']) == 1

    path.write_text('backends: {out: {type: tty}}\nexchanges: {COINBASE: {channels: {trades: [BTC-USD]}}}\n')
    assert cli.main(['health', str(path)]) == 0
    assert 'not configured' in capsys.readouterr().out


@pytest.mark.parametrize('argv', [['--version'], ['-h']])
def test_cli_help_and_version(argv, capsys):
    with pytest.raises(SystemExit) as exit_info:
        cli.main(argv)
    assert exit_info.value.code == 0


def publishing_run(writer, count, delay=0.2):
    '''a stand-in for Feed.run: pushes `count` trades through one stream writer on the worker's own loop, then idles'''
    async def run(stop_event):
        await asyncio.sleep(delay)   # give the test time to connect a client
        async with asyncio.TaskGroup() as tg:
            writer.start_writer(tg, name='test.writer')
            for i in range(count):
                await writer(Trade('TESTEX', 'BTC-USD', BUY, Decimal('1'), Decimal('100') + i, time.time(), id=str(i)), time.time())
            await writer.stop()
        await stop_event.wait()
    return run


async def test_workers_shard_feeds_and_share_the_stream_server(base_config):
    base_config['backends']['stream'] = {'type': 'stream', 'tcp': '127.0.0.1:0'}
    base_config['exchanges']['COINBASE']['channels'] = {'trades': ['BTC-USD', 'ETH-USD', 'LTC-USD', 'SOL-USD']}
    base_config['workers'] = {'count': 2, 'min_symbols': 1}
    base_config['stats_interval'] = 0.05
    base_config['cryptofeed'] = {'uvloop': False}
    app = Cryptostore(parse(base_config))
    workers = app.build()

    assert [worker.plan.labels for worker in workers] == [['COINBASE[1/2]'], ['COINBASE[2/2]']]
    assert workers[0].handler.feeds[0]._init_subscription == {TRADES: ['BTC-USD', 'LTC-USD']}
    assert workers[1].handler.feeds[0]._init_subscription == {TRADES: ['ETH-USD', 'SOL-USD']}
    # each worker has its own writers (a queue belongs to one loop) but they share the one stream server
    first, second = (worker.builder.writer('stream', TRADES) for worker in workers)
    assert first is not second and first.server is second.server and list(app.servers) == ['stream']
    assert 'COINBASE[2/2]' in app.describe() and '2 worker(s)' in app.describe()
    for worker in workers:
        worker.handler.feeds[0].run = publishing_run(worker.builder.writer('stream', TRADES), 2)

    async def client():
        await asyncio.sleep(0.1)
        reader, writer = await asyncio.open_connection('127.0.0.1', app.servers['stream'].tcp_port)
        frames = []
        async with asyncio.timeout(5):
            while len(frames) < 4:
                frames.append(json.loads(await reader.readline()))
        writer.close()
        app.request_stop()
        return frames

    async with asyncio.TaskGroup() as tg:
        task = tg.create_task(client())
        await app.run_async()

    frames = task.result()
    assert sorted(frame['data']['id'] for frame in frames) == ['0', '0', '1', '1']
    assert {frame['type'] for frame in frames} == {'trades'}
    snap = app.snapshot()
    assert set(snap['workers']) == {'0', '1'} and snap['feeds'] == ['COINBASE[1/2]', 'COINBASE[2/2]']
    assert snap['backends']['stream']['channels']['trades']['written'] == 4     # summed over both workers' writers
    assert snap['backends']['stream']['published'] == 4
    assert all(worker.error is None and not worker.thread.is_alive() for worker in workers)


async def test_worker_failure_stops_the_others(base_config):
    base_config['exchanges']['COINBASE']['channels'] = {'trades': ['BTC-USD', 'ETH-USD']}
    base_config['workers'] = {'count': 2, 'min_symbols': 1}
    base_config['stats_interval'] = 0
    base_config['cryptofeed'] = {'uvloop': False}
    app = Cryptostore(parse(base_config))
    workers = app.build()
    assert len(workers) == 2

    async def idle(stop_event):
        await stop_event.wait()

    async def explode(stop_event):
        await asyncio.sleep(0.1)
        raise RuntimeError('the exchange rejected us for good')

    workers[0].handler.feeds[0].run = idle
    workers[1].handler.feeds[0].run = explode

    with pytest.raises(Exception) as info:
        async with asyncio.timeout(15):
            await app.run_async()
    assert 'rejected us for good' in repr(info.value)
    assert workers[1].error is not None and workers[0].error is None     # worker 0 was stopped, not failed
    assert all(worker.done.done() and not worker.thread.is_alive() for worker in workers)


async def test_stop_before_the_workers_start_is_not_lost(base_config):
    base_config['stats_interval'] = 0
    base_config['cryptofeed'] = {'uvloop': False}
    app = Cryptostore(parse(base_config))
    (worker,) = app.build()

    async def idle(stop_event):
        await stop_event.wait()
    worker.handler.feeds[0].run = idle

    app.request_stop()      # before run_async: the worker must notice and exit at once
    async with asyncio.timeout(10):
        await app.run_async()
    assert worker.error is None and not worker.thread.is_alive()


async def test_loop_lag_measures_a_blocked_loop():
    lag = LoopLag(interval=0.02)
    task = asyncio.create_task(lag.run())
    await asyncio.sleep(0.05)
    time.sleep(0.15)        # noqa: ASYNC251 - deliberately block the loop
    await asyncio.sleep(0.05)
    task.cancel()
    assert lag.last >= 0 and lag.take_peak() >= 0.1
    assert lag.take_peak() == 0.0


def test_build_expands_symbol_patterns(base_config):
    Symbols.set('COINBASE', {'BTC-USD': 'BTC-USD', 'ETH-USD': 'ETH-USD', 'BTC-EUR': 'BTC-EUR'}, {})
    base_config['exchanges']['COINBASE']['channels'] = {'trades': '*-USD', 'ticker': ['all'], 'l2_book': ['BTC-EUR', 'BTC-*']}
    app = Cryptostore(parse(base_config))
    (worker,) = app.build()
    feed = worker.handler.feeds[0]
    assert feed._init_subscription == {TRADES: ['BTC-USD', 'ETH-USD'], TICKER: ['BTC-EUR', 'BTC-USD', 'ETH-USD'], L2_BOOK: ['BTC-EUR', 'BTC-USD']}
    description = app.describe()
    assert 'from *-USD' in description and 'from all' in description and 'from BTC-*' in description

    base_config['exchanges']['COINBASE']['channels'] = {'trades': ['*-JPY']}
    with pytest.raises(ConfigError, match='matches none'):
        Cryptostore(parse(base_config)).build()


def test_cli_check_with_patterns_and_workers(tmp_path, capsys):
    Symbols.set('COINBASE', {'BTC-USD': 'BTC-USD', 'ETH-USD': 'ETH-USD', 'BTC-EUR': 'BTC-EUR'}, {})
    path = tmp_path / 'config.yaml'
    path.write_text('backends: {out: {type: tty}}\nexchanges: {COINBASE: {channels: {trades: all}}}\nworkers: 2\n')
    assert cli.main(['check', '--symbols', str(path)]) == 0
    out = capsys.readouterr().out
    assert '3 symbol(s) from all' in out and '1 worker(s) of 2 configured' in out and 'symbols: ok' in out
