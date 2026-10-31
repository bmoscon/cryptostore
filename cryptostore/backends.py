'''
Copyright (C) 2018-2026 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.


The backend registry: every destination data can be routed to, and how a (backend, channel) pair
turns into a writer instance.

The storage types are cryptofeed's own backend classes. Their modules are imported lazily, so a config
that never mentions Postgres never imports asyncpg. The stream and tty types live in cryptostore.

One writer is created per (backend, channel) and shared by every exchange routed to it,
which is what cryptofeed's shared writers are designed for - one connection pool and one writer
task per backend and data type, regardless of how many exchanges feed it.
'''
from dataclasses import dataclass, field
from datetime import datetime, timezone
from importlib import import_module

from cryptofeed.defines import CANDLES, FUNDING, INDEX, L1_BOOK, L2_BOOK, L3_BOOK, LIQUIDATIONS, OPEN_INTEREST, TICKER, TRADES


BOOK_CHANNELS = (L2_BOOK, L3_BOOK)
ALL_CHANNELS = (L1_BOOK, L2_BOOK, L3_BOOK, TRADES, TICKER, FUNDING, OPEN_INTEREST, LIQUIDATIONS, INDEX, CANDLES)
BOOK_OPTIONS = ('snapshot_interval', 'snapshots_only')

# cryptofeed names its per-type classes <Type><Family>. these are the type prefixes.
_PREFIX = {
    TRADES: 'Trade',
    TICKER: 'Ticker',
    FUNDING: 'Funding',
    OPEN_INTEREST: 'OpenInterest',
    LIQUIDATIONS: 'Liquidations',
    CANDLES: 'Candles',
    INDEX: 'Index',
    L1_BOOK: 'L1Book',
}


class BackendError(ValueError):
    '''a backend cannot be built - missing dependency or an unsupported combination'''


def _camel(channel: str) -> str:
    return ''.join(part.capitalize() for part in channel.split('_'))


@dataclass(frozen=True)
class BackendType:
    name: str
    module: str                                   # module holding the backend classes
    family: str                                   # class name suffix, e.g. 'Redis' -> TradeRedis
    book_class: str = None                        # class for l2/l3 books, None when books are unsupported
    generic_base: str = None                      # base class to synthesize missing channels from
    builtin: tuple = ()                           # channels the module defines classes for (beyond the family pattern)
    excluded: tuple = ()                          # channels this backend can never take
    aliases: dict = field(default_factory=dict)   # friendly option name -> constructor keyword
    required: tuple = ()                          # constructor keywords that must be present
    book_options: tuple = BOOK_OPTIONS
    extra: str = None                             # pip extra that provides the dependency
    description: str = ''
    validator: str = None                         # dotted path of a function(options) that raises ValueError

    def channels(self) -> tuple:
        return tuple(channel for channel in ALL_CHANNELS if self.supports(channel))

    def supports(self, channel: str) -> bool:
        if channel in self.excluded:
            return False
        if channel in BOOK_CHANNELS:
            return self.book_class is not None
        if channel in self.builtin or self.generic_base is not None:
            return True
        return False

    def validate(self, options: dict):
        missing = [key for key in self.required if key not in options]
        if missing:
            raise ValueError(f'{self.name} backends require {", ".join(missing)}')
        if self.validator is not None:
            module, _, function = self.validator.rpartition('.')
            getattr(import_module(module), function)(options)

    def load_module(self):
        try:
            return import_module(self.module)
        except ImportError as e:
            hint = f' - install it with: pip install cryptostore[{self.extra}]' if self.extra else ''
            raise BackendError(f'the {self.name} backend needs a package that is not installed ({e}){hint}') from None

    def backend_class(self, channel: str):
        if not self.supports(channel):
            raise BackendError(f'{self.name} backends cannot store {channel}')
        module = self.load_module()
        if channel in BOOK_CHANNELS:
            return getattr(module, self.book_class)

        name = f'{_PREFIX[channel]}{self.family}'
        existing = getattr(module, name, None)
        if existing is not None:
            return existing

        # cryptofeed's per-type classes differ only in default_key, so a missing one is synthesized
        # the same way the hand-written ones are declared: class TradeX(XCallback, BackendCallback)
        from cryptofeed.backends.backend import BackendCallback
        base = getattr(module, self.generic_base)
        return type(f'{_camel(channel)}{self.family}', (base, BackendCallback), {'default_key': channel, '__module__': __name__})


_SOCKET_ALIASES = {'host': 'addr', 'url': 'addr', 'address': 'addr'}

BACKEND_TYPES = {
    'redis': BackendType(
        name='redis', module='cryptofeed.backends.redis', family='Redis', book_class='BookRedis',
        generic_base='RedisZSetCallback', extra='redis',
        description='Redis sorted sets, one per exchange/symbol, scored by timestamp'),
    'redis_stream': BackendType(
        name='redis_stream', module='cryptofeed.backends.redis', family='Stream', book_class='BookStream',
        generic_base='RedisStreamCallback', extra='redis',
        description='Redis Streams (XADD), one stream per exchange/symbol'),
    'mongo': BackendType(
        name='mongo', module='cryptofeed.backends.mongo', family='Mongo', book_class='BookMongo',
        generic_base='MongoCallback', aliases={'database': 'db'}, required=('db',), extra='mongo',
        description='MongoDB, one collection per data type'),
    'postgres': BackendType(
        name='postgres', module='cryptofeed.backends.postgres', family='Postgres', book_class='BookPostgres',
        builtin=(TRADES, TICKER, FUNDING, OPEN_INTEREST, LIQUIDATIONS, CANDLES, INDEX),
        aliases={'password': 'pw', 'database': 'db'}, required=('db',), extra='postgres',
        description='PostgreSQL, one table per data type (tables must exist - see the README)'),
    'influxdb': BackendType(
        name='influxdb', module='cryptofeed.backends.influxdb', family='Influx', book_class='BookInflux',
        generic_base='InfluxCallback', aliases=_SOCKET_ALIASES, required=('addr', 'org', 'bucket', 'token'),
        description='InfluxDB 2.x over the HTTP write API'),
    'questdb': BackendType(
        name='questdb', module='cryptofeed.backends.quest', family='Quest', book_class='BookQuest',
        generic_base='QuestCallback', book_options=('depth',),
        description='QuestDB over the HTTP line-protocol endpoint (books are stored as top-N levels)'),
    'kafka': BackendType(
        name='kafka', module='cryptofeed.backends.kafka', family='Kafka', book_class='BookKafka',
        generic_base='KafkaCallback', extra='kafka',
        description='Kafka, one topic per data type/exchange/symbol. Options are passed to AIOKafkaProducer'),
    'zmq': BackendType(
        name='zmq', module='cryptofeed.backends.zmq', family='ZMQ', book_class='BookZMQ',
        generic_base='ZMQCallback', extra='zmq',
        description='ZeroMQ PUB that connects to a subscriber you bind. For a server that clients connect to use the stream backend'),
    'socket': BackendType(
        name='socket', module='cryptofeed.backends.socket', family='Socket', book_class='BookSocket',
        generic_base='SocketCallback', aliases=_SOCKET_ALIASES, required=('addr',),
        description='Connects to a tcp://, udp:// or uds:// listener you run. For a server that clients connect to use the stream backend'),
    'stream': BackendType(
        name='stream', module='cryptostore.stream', family='Stream', book_class='BookStreamCallback',
        generic_base='StreamCallback', extra='zmq', validator='cryptostore.stream.validate_options',
        description='cryptostore streaming server - binds TCP, Unix socket and/or ZeroMQ PUB endpoints that your applications connect to'),
    'tty': BackendType(
        name='tty', module='cryptostore.backends', family='TTY', book_class='TTY', generic_base='TTY', book_options=(),
        validator='cryptostore.backends.validate_tty_options', description='prints every update to stdout, for debugging'),
}
BACKEND_TYPES['stdout'] = BACKEND_TYPES['tty']


def validate_tty_options(options: dict):
    if options:
        raise ValueError(f'tty backends take no options, got {", ".join(options)}')


class TTY:
    '''debug backend - prints every update. Not a BackendQueue, so it has no stats'''

    def __init__(self, **kwargs):
        pass

    async def __call__(self, obj, receipt_timestamp: float):
        stamp = datetime.fromtimestamp(receipt_timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
        print(f'{stamp} {obj}', flush=True)


class BackendBuilder:
    '''
    Turns BackendConfigs into writer instances, one per (backend, channel), and owns the stream
    servers the stream writers publish through.
    '''

    def __init__(self, configs: dict, servers: dict = None):
        '''
        servers: the StreamServer registry to share. Every worker gets its own builder (writers are
        bound to the worker's event loop) but they all publish through the same stream servers
        '''
        self.configs = configs      # name -> BackendConfig
        self.servers = servers if servers is not None else {}
        self._writers = {}      # (name, channel) -> writer instance

    def writer(self, backend_name: str, channel: str):
        key = (backend_name, channel)
        if key not in self._writers:
            self._writers[key] = self._build(self.configs[backend_name], channel)
        return self._writers[key]

    def writers(self) -> dict:
        return dict(self._writers)

    def _build(self, backend, channel: str):
        backend_type = backend.type
        if backend_type.name == 'tty':
            return TTY()

        cls = backend_type.backend_class(channel)
        kwargs = dict(backend.options)
        kwargs.update(backend.queue)
        if channel in BOOK_CHANNELS:
            kwargs.update(backend.book)

        if backend_type.name == 'stream':
            from cryptostore.stream import StreamServer
            kwargs = {key: value for key, value in kwargs.items() if key not in StreamServer.SERVER_OPTIONS}
            kwargs['server'] = self._server(backend)
            kwargs['key'] = channel

        try:
            return cls(**kwargs)
        except TypeError as e:
            raise BackendError(f'backend {backend.name!r} ({backend_type.name}): {e}') from None

    def _server(self, backend):
        if backend.name not in self.servers:
            from cryptostore.stream import StreamServer
            options = {key: value for key, value in backend.options.items() if key in StreamServer.SERVER_OPTIONS}
            self.servers[backend.name] = StreamServer(name=backend.name, **options)
        return self.servers[backend.name]


def describe() -> str:
    seen = set()
    lines = []
    for name, backend_type in BACKEND_TYPES.items():
        if backend_type.name in seen:
            continue
        seen.add(backend_type.name)
        lines.append(f'{name:<13} {backend_type.description}')
        lines.append(f'{"":<13} channels: {", ".join(backend_type.channels())}')
    return '\n'.join(lines)

