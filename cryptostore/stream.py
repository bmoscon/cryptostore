'''
Copyright (C) 2018-2026 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.


The streaming server: cryptostore binds the endpoints and your applications connect to them.

cryptofeed's socket and zmq backends do the reverse (they connect out to a listener you run), which is
awkward for a long lived collector. StreamServer listens on any of

  * TCP            newline-delimited JSON frames: {"type": "<channel>", "data": {...}}\n
  * Unix socket    the same frames
  * ZeroMQ PUB     "<exchange>-<channel>-<symbol> <json>"  (the format cryptofeed's zmq backend emits,
                   so a SUB socket can subscribe by prefix)

The TCP and Unix frames are byte-for-byte what cryptofeed's socket backend writes, so a reader written
for one works for the other.

Each TCP/Unix client has its own bounded queue. A client that falls more than client_queue frames behind
is disconnected rather than silently fed a gap - a client building a book from deltas needs to know it
missed data. ZeroMQ applies its own high-water mark and drops for slow subscribers, as PUB sockets do.

One server is shared by every worker thread: writers encode on their own loop and hand the frames over
with submit(), which runs the fan-out on the server's loop. Nothing here is touched from two threads.
'''
import asyncio
import logging
import os
from collections import defaultdict
from contextlib import suppress
from decimal import Decimal

from cryptofeed import _json as json
from cryptofeed.backends.backend import BackendBookCallback, BackendCallback, BackendQueue


LOG = logging.getLogger('cryptostore.stream')
MAX_UDS_PATH = 103   # 104 bytes on macOS and 108 on Linux, minus the terminator


def parse_tcp(spec) -> tuple:
    '''"host:port", ":port" or a bare port -> (host, port)'''
    if isinstance(spec, int) and not isinstance(spec, bool):
        return '0.0.0.0', spec
    if not isinstance(spec, str):
        raise ValueError(f'tcp must be "host:port", not {spec!r}')
    host, sep, port = spec.rpartition(':')
    if not sep:
        host, port = '', spec
    host = host.strip('[]') or '0.0.0.0'
    try:
        port = int(port)
    except ValueError:
        raise ValueError(f'tcp port must be a number, not {port!r}') from None
    if not 0 <= port < 65536:
        raise ValueError(f'tcp port {port} is out of range')
    return host, port


def validate_options(options: dict):
    '''raises ValueError for anything StreamServer would reject - run at config time, before anything connects'''
    unknown = [key for key in options if key not in StreamServer.OPTIONS]
    if unknown:
        raise ValueError(f'unknown stream option(s) {", ".join(unknown)} - valid options are {", ".join(StreamServer.OPTIONS)}')
    StreamServer(**{key: value for key, value in options.items() if key in StreamServer.SERVER_OPTIONS})


class _Client:
    __slots__ = ('writer', 'queue', 'name', 'kicked', 'sent')

    def __init__(self, writer: asyncio.StreamWriter, name: str, maxsize: int):
        self.writer = writer
        self.queue = asyncio.Queue(maxsize=maxsize)
        self.name = name
        self.kicked = False
        self.sent = 0


class StreamServer:
    SERVER_OPTIONS = ('tcp', 'uds', 'zmq', 'client_queue', 'zmq_hwm')
    OPTIONS = SERVER_OPTIONS + ('numeric_type', 'none_to')

    def __init__(self, name: str = 'stream', tcp=None, uds: str = None, zmq: str = None, client_queue: int = 10_000, zmq_hwm: int = 100_000):
        if tcp is None and uds is None and zmq is None:
            raise ValueError('a stream backend needs at least one of tcp, uds or zmq')
        if not isinstance(client_queue, int) or isinstance(client_queue, bool) or client_queue < 1:
            raise ValueError(f'client_queue must be a positive integer, not {client_queue!r}')
        if not isinstance(zmq_hwm, int) or isinstance(zmq_hwm, bool) or zmq_hwm < 0:
            raise ValueError(f'zmq_hwm must be a non-negative integer, not {zmq_hwm!r}')
        if uds is not None and (not isinstance(uds, str) or not uds):
            raise ValueError(f'uds must be a socket path, not {uds!r}')
        if uds is not None and len(uds.encode()) > MAX_UDS_PATH:
            raise ValueError(f'uds path is {len(uds.encode())} bytes - unix socket paths are limited to {MAX_UDS_PATH}')
        if zmq is not None and (not isinstance(zmq, str) or '://' not in zmq):
            raise ValueError(f'zmq must be a ZeroMQ endpoint such as tcp://0.0.0.0:5555, not {zmq!r}')
        if zmq is not None:
            try:
                import zmq as _zmq  # noqa: F401
            except ImportError:
                raise ValueError('the zmq endpoint needs pyzmq - install it with: pip install cryptostore[zmq]') from None

        self.name = name
        self.tcp = parse_tcp(tcp) if tcp is not None else None
        self.uds = uds
        self.zmq = zmq
        self.client_queue = client_queue
        self.zmq_hwm = zmq_hwm
        self._servers = []
        self._clients = set()
        self._tasks = set()
        self._pub = None
        self._ctx = None
        self._zmq = None
        self._loop = None
        self.published = 0
        self.kicked = 0
        self.connections = 0

    @property
    def clients(self) -> int:
        return len(self._clients)

    @property
    def endpoints(self) -> list:
        out = []
        if self.tcp:
            out.append(f'tcp://{self.tcp[0]}:{self.tcp[1]}')
        if self.uds:
            out.append(f'uds://{self.uds}')
        if self.zmq:
            out.append(f'zmq+pub://{self.zmq}')
        return out

    @property
    def tcp_port(self):
        '''the bound TCP port - useful when configured with port 0'''
        for server in self._servers:
            for sock in server.sockets:
                if sock.family.name in ('AF_INET', 'AF_INET6'):
                    return sock.getsockname()[1]
        return None

    async def start(self):
        if self.tcp is not None:
            host, port = self.tcp
            self._servers.append(await asyncio.start_server(self._on_connect, host=host, port=port, reuse_address=True))
        if self.uds is not None:
            with suppress(FileNotFoundError):
                os.unlink(self.uds)
            self._servers.append(await asyncio.start_unix_server(self._on_connect, path=self.uds))
        if self.zmq is not None:
            import zmq
            self._zmq = zmq
            self._ctx = zmq.Context()
            self._pub = self._ctx.socket(zmq.PUB)   # only ever used from the server loop, so a plain socket will do
            self._pub.setsockopt(zmq.SNDHWM, self.zmq_hwm)
            self._pub.setsockopt(zmq.LINGER, 0)
            self._pub.bind(self.zmq)
        self._loop = asyncio.get_running_loop()
        LOG.info('stream %s: listening on %s', self.name, ', '.join(self.endpoints))

    async def close(self):
        for server in self._servers:
            server.close()
        for client in list(self._clients):
            self._drop(client, reason='server shutting down')
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        for server in self._servers:
            with suppress(Exception):
                await server.wait_closed()
        self._servers = []
        if self._pub is not None:
            self._pub.close(linger=0)
            self._pub = None
        if self._ctx is not None:
            self._ctx.term()
            self._ctx = None
        if self.uds is not None:
            with suppress(FileNotFoundError):
                os.unlink(self.uds)
        self._loop = None
        LOG.info('stream %s: closed (%d frames published, %d client connections, %d kicked)', self.name, self.published, self.connections, self.kicked)

    def submit(self, frames: list):
        '''
        hand encoded (topic, payload) frames over from any thread. The fan-out runs on the server's
        own loop, which is what lets every worker share one stream backend
        '''
        loop = self._loop
        try:
            current = asyncio.get_running_loop()
        except RuntimeError:
            current = None
        if loop is None or current is loop:
            self.publish_many(frames)
            return
        try:
            loop.call_soon_threadsafe(self.publish_many, frames)
        except RuntimeError:   # the server loop has closed - shutting down
            pass

    def publish_many(self, frames: list):
        '''fan encoded frames out to every connected client. Server loop only - use submit() elsewhere'''
        self.published += len(frames)
        if self._clients:
            clients = list(self._clients)
            for _, payload in frames:
                line = payload + b'\n'
                for client in clients:
                    if client.kicked:
                        continue
                    try:
                        client.queue.put_nowait(line)
                    except asyncio.QueueFull:
                        self.kicked += 1
                        self._drop(client, reason=f'fell {self.client_queue} frames behind')
        if self._pub is not None:
            for topic, payload in frames:
                try:
                    self._pub.send(topic + b' ' + payload, self._zmq.NOBLOCK)
                except self._zmq.ZMQError as e:
                    LOG.warning('stream %s: zmq publish failed: %s', self.name, e)

    async def publish(self, topic: str, payload: bytes):
        '''one frame, from the server's loop'''
        self.publish_many([(topic.encode(), payload)])

    async def _on_connect(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info('peername') or writer.get_extra_info('sockname') or 'unix'
        name = f'{peer[0]}:{peer[1]}' if isinstance(peer, tuple) and len(peer) >= 2 else str(peer)
        client = _Client(writer, name, self.client_queue)
        self._clients.add(client)
        self.connections += 1
        LOG.info('stream %s: client %s connected (%d connected)', self.name, name, len(self._clients))

        handler = asyncio.current_task()
        self._tasks.add(handler)
        pump = asyncio.create_task(self._pump(client), name=f'stream.{self.name}.pump.{name}')
        eof = asyncio.create_task(self._until_eof(reader), name=f'stream.{self.name}.eof.{name}')
        try:
            await asyncio.wait({pump, eof}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            pump.cancel()
            eof.cancel()
            await asyncio.gather(pump, eof, return_exceptions=True)
            self._tasks.discard(handler)
            self._drop(client, reason=None)

    async def _until_eof(self, reader: asyncio.StreamReader):
        # clients never need to send anything - reading just notices when they go away
        with suppress(Exception):
            while await reader.read(4096):
                pass

    async def _pump(self, client: _Client):
        queue, writer = client.queue, client.writer
        try:
            while not client.kicked:
                line = await queue.get()
                writer.write(line)
                client.sent += 1
                while not queue.empty():
                    writer.write(queue.get_nowait())
                    client.sent += 1
                await writer.drain()
        except (ConnectionError, OSError):
            pass

    def _drop(self, client: _Client, reason):
        if client not in self._clients:
            return
        self._clients.discard(client)
        client.kicked = True
        transport = client.writer.transport
        if reason:
            LOG.warning('stream %s: disconnecting client %s - %s (%d frames sent)', self.name, client.name, reason, client.sent)
            with suppress(Exception):
                transport.abort()
        else:
            LOG.info('stream %s: client %s disconnected after %d frames (%d connected)', self.name, client.name, client.sent, len(self._clients))
            with suppress(Exception):
                transport.close()


class StreamCallback(BackendQueue):
    '''per-channel backend: encodes updates once and hands them to the shared StreamServer'''
    hand_off = True
    default_key = 'stream'

    def __init__(self, server: StreamServer, key: str = None, numeric_type=Decimal, none_to=None, **kwargs):
        super().__init__(**kwargs)
        self.server = server
        self.key = key if key else self.default_key
        self.numeric_type = numeric_type
        self.none_to = none_to

    async def write_batch(self, batch: list):
        key = self.key
        frames = [(f'{update["exchange"]}-{key}-{update["symbol"]}'.encode(), json.dumpb({'type': key, 'data': update})) for update in batch]
        self.server.submit(frames)


class TradeStream(StreamCallback, BackendCallback):
    default_key = 'trades'


class TickerStream(StreamCallback, BackendCallback):
    default_key = 'ticker'


class FundingStream(StreamCallback, BackendCallback):
    default_key = 'funding'


class OpenInterestStream(StreamCallback, BackendCallback):
    default_key = 'open_interest'


class LiquidationsStream(StreamCallback, BackendCallback):
    default_key = 'liquidations'


class CandlesStream(StreamCallback, BackendCallback):
    default_key = 'candles'


class IndexStream(StreamCallback, BackendCallback):
    default_key = 'index'


class L1BookStream(StreamCallback, BackendCallback):
    default_key = 'l1_book'


class BookStreamCallback(StreamCallback, BackendBookCallback):
    default_key = 'l2_book'

    def __init__(self, *args, snapshots_only=False, snapshot_interval=1000, **kwargs):
        self.snapshots_only = snapshots_only
        self.snapshot_interval = snapshot_interval
        self.snapshot_count = defaultdict(int)
        super().__init__(*args, **kwargs)
