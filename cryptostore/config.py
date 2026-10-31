'''
Copyright (C) 2018-2026 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.


Loads and validates the cryptostore config file.

The file is YAML (JSON is valid YAML, so a .json file works too). Every string value may
reference an environment variable as ${NAME} or ${NAME:-default}, which is how secrets reach
a container. Validation is strict: an unknown key, exchange, channel or backend is an error with
a suggestion, so a typo fails at start-up rather than silently collecting nothing.
'''
import inspect
import json
import os
import re
from dataclasses import dataclass, field
from decimal import Decimal
from difflib import get_close_matches

import yaml
from cryptofeed.backends.backend import RetryPolicy
from cryptofeed.defines import CANDLES, FUNDING, INDEX, L1_BOOK, L2_BOOK, L3_BOOK, LIQUIDATIONS, OPEN_INTEREST, TICKER, TRADES
from cryptofeed.exchanges import EXCHANGE_MAP

from cryptostore.backends import BACKEND_TYPES, BackendType


CHANNELS = (L1_BOOK, L2_BOOK, L3_BOOK, TRADES, TICKER, FUNDING, OPEN_INTEREST, LIQUIDATIONS, INDEX, CANDLES)
NUMERIC_TYPES = {'float': float, 'decimal': Decimal, 'str': str}
QUEUE_OPTIONS = ('max_depth', 'overflow', 'batch_max', 'batch_interval', 'flush_deadline', 'retry')
RETRY_OPTIONS = ('max_attempts', 'base', 'jitter', 'max_delay')
ON_FEED_ERROR = ('raise', 'remove_feed')
LOG_LEVELS = ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
TOP_LEVEL = ('exchanges', 'backends', 'workers', 'record', 'log', 'stats_interval', 'health_file', 'on_feed_error', 'cryptofeed')
EXCHANGE_KEYS = ('channels', 'backends', 'options', 'shards')
WORKERS_KEYS = ('count', 'min_symbols', 'lag_warn')
CHANNEL_KEYS = ('symbols', 'backends')
RECORD_KEYS = ('path', 'rotate_size', 'compress')
LOG_KEYS = ('level', 'file')
# feed constructor arguments cryptostore owns - a config may not set them under options
RESERVED_FEED_OPTIONS = frozenset(('symbols', 'channels', 'subscription', 'callbacks', 'config'))

DEFAULT_CONFIG_PATH = 'config.yaml'
CONFIG_ENV = 'CRYPTOSTORE_CONFIG'

_ENV_REF = re.compile(r'\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}')
ALL_SYMBOLS = 'all'
_PATTERN_CHARS = frozenset('*?[')


def is_pattern(symbol: str) -> bool:
    '''`all` or a glob (`*-USDT`, `BTC-*`) - expanded against the exchange's symbol list at start-up'''
    return symbol.lower() == ALL_SYMBOLS or any(char in symbol for char in _PATTERN_CHARS)


def cpu_count() -> int:
    return os.process_cpu_count() or os.cpu_count() or 1


class ConfigError(ValueError):
    '''the config file is invalid - the message says where and why'''


@dataclass(frozen=True)
class BackendConfig:
    name: str
    type: BackendType
    options: dict = field(default_factory=dict)   # keyword arguments for the backend constructors
    book: dict = field(default_factory=dict)      # extra keyword arguments for the l2/l3 book backends
    queue: dict = field(default_factory=dict)     # cryptofeed BackendQueue tuning


@dataclass(frozen=True)
class ChannelConfig:
    channel: str
    symbols: tuple
    backends: tuple
    patterns: tuple = ()                          # the `all`/glob entries symbols came from, once expanded


@dataclass(frozen=True)
class ExchangeConfig:
    name: str
    channels: tuple
    options: dict = field(default_factory=dict)   # keyword arguments for the cryptofeed Feed
    shards: int = None                            # at most this many shards when split across workers

    @property
    def subscription(self) -> dict:
        return {channel.channel: list(channel.symbols) for channel in self.channels}

    @property
    def patterns(self) -> tuple:
        return tuple(pattern for channel in self.channels for pattern in channel.patterns)


@dataclass(frozen=True)
class WorkersConfig:
    count: int = 1              # worker threads, each running one FeedHandler on its own event loop
    min_symbols: int = 50       # an exchange is split only into shards of at least this many symbols
    lag_warn: float = 1.0       # seconds of event loop lag that log a warning


@dataclass(frozen=True)
class RecordConfig:
    path: str
    rotate_size: int = None
    compress: bool = True


@dataclass(frozen=True)
class Config:
    exchanges: tuple
    backends: dict
    record: RecordConfig = None
    log_level: str = 'INFO'
    log_file: str = None
    stats_interval: float = 60.0
    health_file: str = None
    on_feed_error: str = 'raise'
    cryptofeed: dict = field(default_factory=dict)
    workers: WorkersConfig = field(default_factory=WorkersConfig)

    def exchanges_with_patterns(self) -> list:
        return [exchange.name for exchange in self.exchanges if exchange.patterns]

    def backends_used(self) -> list:
        used = []
        for exchange in self.exchanges:
            for channel in exchange.channels:
                for backend in channel.backends:
                    if backend not in used:
                        used.append(backend)
        return used


def config_path(path: str = None) -> str:
    return path or os.environ.get(CONFIG_ENV) or DEFAULT_CONFIG_PATH


def load(path: str = None) -> Config:
    path = config_path(path)
    try:
        with open(path) as fp:
            if path.endswith('.json'):
                raw = json.load(fp)
            else:
                raw = yaml.safe_load(fp)
    except FileNotFoundError:
        raise ConfigError(f'config file {path!r} not found (set {CONFIG_ENV} or pass the path on the command line)') from None
    except (yaml.YAMLError, ValueError) as e:
        raise ConfigError(f'{path}: not valid YAML/JSON: {e}') from None
    return parse(raw, source=path)


def parse(raw, source: str = 'config') -> Config:
    if raw is None:
        raise ConfigError(f'{source}: the config is empty')
    _expect_mapping(raw, source)
    raw = _expand_env(raw, source)
    _check_keys(raw, TOP_LEVEL, source)

    backends = _parse_backends(raw.get('backends'), source)
    exchanges = _parse_exchanges(raw.get('exchanges'), backends, source)
    record = _parse_record(raw.get('record'), source)
    log_level, log_file = _parse_log(raw.get('log'), source)
    workers = _parse_workers(raw.get('workers'), source)
    if record is not None and workers.count > 1:
        raise ConfigError(f'{source}: record needs workers: 1 - cryptofeed\'s recorder is bound to one event loop')

    stats_interval = raw.get('stats_interval', 60)
    if not isinstance(stats_interval, (int, float)) or isinstance(stats_interval, bool) or stats_interval < 0:
        raise ConfigError(f'{source}: stats_interval must be a number of seconds (0 disables), not {stats_interval!r}')

    health_file = raw.get('health_file')
    if health_file is not None and not isinstance(health_file, str):
        raise ConfigError(f'{source}: health_file must be a path')

    on_feed_error = raw.get('on_feed_error', 'raise')
    if on_feed_error not in ON_FEED_ERROR:
        raise ConfigError(f'{source}: on_feed_error must be one of {", ".join(ON_FEED_ERROR)}, not {on_feed_error!r}')

    cryptofeed = raw.get('cryptofeed') or {}
    _expect_mapping(cryptofeed, f'{source}: cryptofeed')
    if 'log' in cryptofeed:
        raise ConfigError(f'{source}: cryptofeed.log is managed by cryptostore - use the top-level log section')

    return Config(exchanges=exchanges, backends=backends, record=record, log_level=log_level, log_file=log_file,
                  stats_interval=float(stats_interval), health_file=health_file, on_feed_error=on_feed_error,
                  cryptofeed=dict(cryptofeed), workers=workers)


def feed_options(exchange_class) -> set:
    '''every keyword the exchange's constructor chain accepts, minus the ones cryptostore sets itself'''
    names = set()
    for klass in exchange_class.__mro__:
        init = klass.__dict__.get('__init__')
        if init is None:
            continue
        for param in inspect.signature(init).parameters.values():
            if param.name == 'self' or param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
                continue
            names.add(param.name)
    return names - RESERVED_FEED_OPTIONS


def resolve_exchange(name: str) -> str:
    '''the EXCHANGE_MAP key for a config name, case-insensitively, or a ConfigError with a suggestion'''
    if not isinstance(name, str):
        raise ConfigError(f'exchange names must be strings, not {name!r}')
    upper = name.upper()
    if upper in EXCHANGE_MAP:
        return upper
    raise ConfigError(f'unknown exchange {name!r}{_hint(upper, EXCHANGE_MAP)}')


def _parse_backends(raw, source: str) -> dict:
    if raw is None:
        raise ConfigError(f'{source}: at least one backend is required under "backends"')
    _expect_mapping(raw, f'{source}: backends')
    if not raw:
        raise ConfigError(f'{source}: at least one backend is required under "backends"')

    backends = {}
    for name, body in raw.items():
        where = f'{source}: backends.{name}'
        if not isinstance(name, str) or not name:
            raise ConfigError(f'{source}: backend names must be non-empty strings, not {name!r}')
        _expect_mapping(body, where)
        body = dict(body)

        type_name = body.pop('type', None)
        if not isinstance(type_name, str):
            raise ConfigError(f'{where}: "type" is required - one of {", ".join(sorted(BACKEND_TYPES))}')
        backend_type = BACKEND_TYPES.get(type_name.lower())
        if backend_type is None:
            raise ConfigError(f'{where}: unknown backend type {type_name!r}{_hint(type_name.lower(), BACKEND_TYPES)}')

        book = body.pop('book', None) or {}
        _expect_mapping(book, f'{where}.book')
        _check_keys(book, backend_type.book_options, f'{where}.book')

        queue = _parse_queue(body.pop('queue', None), where)

        options = {}
        for key, value in body.items():
            if not isinstance(key, str):
                raise ConfigError(f'{where}: option names must be strings, not {key!r}')
            key = backend_type.aliases.get(key, key)
            if key in options:
                raise ConfigError(f'{where}: {key!r} is set twice (once through an alias)')
            options[key] = value

        if 'numeric_type' in options:
            numeric = options['numeric_type']
            if not isinstance(numeric, str) or numeric.lower() not in NUMERIC_TYPES:
                raise ConfigError(f'{where}: numeric_type must be one of {", ".join(NUMERIC_TYPES)}, not {numeric!r}')
            options['numeric_type'] = NUMERIC_TYPES[numeric.lower()]

        try:
            backend_type.validate(options)
        except ValueError as e:
            raise ConfigError(f'{where}: {e}') from None

        backends[name] = BackendConfig(name=name, type=backend_type, options=options, book=dict(book), queue=queue)
    return backends


def _parse_queue(raw, where: str) -> dict:
    if raw is None:
        return {}
    where = f'{where}.queue'
    _expect_mapping(raw, where)
    _check_keys(raw, QUEUE_OPTIONS, where)
    queue = dict(raw)

    if 'overflow' in queue and queue['overflow'] not in ('block', 'drop_oldest', 'drop_new'):
        raise ConfigError(f'{where}: overflow must be block, drop_oldest or drop_new, not {queue["overflow"]!r}')
    for key in ('max_depth', 'batch_max'):
        if key in queue and (not isinstance(queue[key], int) or isinstance(queue[key], bool) or queue[key] < 1):
            raise ConfigError(f'{where}: {key} must be a positive integer, not {queue[key]!r}')
    for key in ('batch_interval', 'flush_deadline'):
        if key in queue and (not isinstance(queue[key], (int, float)) or isinstance(queue[key], bool) or queue[key] < 0):
            raise ConfigError(f'{where}: {key} must be a number of seconds, not {queue[key]!r}')

    retry = queue.get('retry')
    if retry is not None:
        _expect_mapping(retry, f'{where}.retry')
        _check_keys(retry, RETRY_OPTIONS, f'{where}.retry')
        try:
            queue['retry'] = RetryPolicy(**retry)
        except TypeError as e:
            raise ConfigError(f'{where}.retry: {e}') from None
    return queue


def _parse_exchanges(raw, backends: dict, source: str) -> tuple:
    if raw is None:
        raise ConfigError(f'{source}: at least one exchange is required under "exchanges"')
    _expect_mapping(raw, f'{source}: exchanges')
    if not raw:
        raise ConfigError(f'{source}: at least one exchange is required under "exchanges"')

    exchanges = []
    seen = set()
    for name, body in raw.items():
        where = f'{source}: exchanges.{name}'
        try:
            exchange_id = resolve_exchange(name)
        except ConfigError as e:
            raise ConfigError(f'{where}: {e}') from None
        if exchange_id in seen:
            raise ConfigError(f'{where}: {exchange_id} is configured twice - put every channel for an exchange under one entry')
        seen.add(exchange_id)
        exchange_class = EXCHANGE_MAP[exchange_id]

        _expect_mapping(body, where)
        _check_keys(body, EXCHANGE_KEYS, where)

        default_backends = _parse_backend_refs(body.get('backends'), backends, where, default=tuple(backends))
        channels = body.get('channels')
        if not channels:
            raise ConfigError(f'{where}: "channels" is required, e.g. channels: {{trades: [BTC-USD]}}')
        _expect_mapping(channels, f'{where}.channels')

        parsed = []
        for channel, spec in channels.items():
            parsed.append(_parse_channel(channel, spec, exchange_class, backends, default_backends, f'{where}.channels'))

        options = body.get('options') or {}
        _expect_mapping(options, f'{where}.options')
        allowed = feed_options(exchange_class)
        for key in options:
            if key in RESERVED_FEED_OPTIONS:
                raise ConfigError(f'{where}.options: {key!r} is set by cryptostore from the channels section')
            if key not in allowed:
                raise ConfigError(f'{where}.options: {exchange_id} does not take an option named {key!r}{_hint(key, allowed)}')

        shards = body.get('shards')
        if shards is not None and (not isinstance(shards, int) or isinstance(shards, bool) or shards < 1):
            raise ConfigError(f'{where}: shards must be a positive integer (the most shards this exchange is split into), not {shards!r}')

        exchanges.append(ExchangeConfig(name=exchange_id, channels=tuple(parsed), options=dict(options), shards=shards))
    return tuple(exchanges)


def _parse_channel(channel, spec, exchange_class, backends: dict, default_backends: tuple, where: str) -> ChannelConfig:
    if not isinstance(channel, str):
        raise ConfigError(f'{where}: channel names must be strings, not {channel!r}')
    channel_id = channel.lower()
    where = f'{where}.{channel}'
    if channel_id not in CHANNELS:
        raise ConfigError(f'{where}: unknown channel{_hint(channel_id, CHANNELS)} - channels are {", ".join(CHANNELS)}')
    supported = exchange_class.websocket_channels
    if channel_id not in supported:
        raise ConfigError(f'{where}: {exchange_class.id} does not provide {channel_id} - it supports {", ".join(supported)}')

    if isinstance(spec, dict):
        _check_keys(spec, CHANNEL_KEYS, where)
        symbols = spec.get('symbols')
        chosen = _parse_backend_refs(spec.get('backends'), backends, where, default=default_backends)
    else:
        symbols = spec
        chosen = default_backends

    if isinstance(symbols, str):
        symbols = [symbols]
    if not isinstance(symbols, list) or not symbols or not all(isinstance(symbol, str) and symbol for symbol in symbols):
        raise ConfigError(f'{where}: expected a list of symbols, e.g. [BTC-USD, ETH-USD], a pattern such as "*-USDT", or all')
    if not chosen:
        raise ConfigError(f'{where}: no backends selected - every channel must route somewhere')

    for backend in chosen:
        backend_type = backends[backend].type
        if not backend_type.supports(channel_id):
            raise ConfigError(f'{where}: backend {backend!r} ({backend_type.name}) cannot store {channel_id} - it supports {", ".join(backend_type.channels())}')

    deduped = tuple(dict.fromkeys(symbols))
    patterns = tuple(symbol for symbol in deduped if is_pattern(symbol))
    return ChannelConfig(channel=channel_id, symbols=deduped, backends=chosen, patterns=patterns)


def _parse_backend_refs(raw, backends: dict, where: str, default: tuple) -> tuple:
    if raw is None:
        return tuple(default)
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        raise ConfigError(f'{where}.backends: expected a list of backend names')
    for name in raw:
        if name not in backends:
            raise ConfigError(f'{where}.backends: {name!r} is not a configured backend{_hint(str(name), backends)}')
    return tuple(dict.fromkeys(raw))


def _parse_workers(raw, source: str) -> WorkersConfig:
    if raw is None:
        return WorkersConfig()
    where = f'{source}: workers'
    if not isinstance(raw, dict):
        raw = {'count': raw}
    _check_keys(raw, WORKERS_KEYS, where)

    count = raw.get('count', 1)
    if isinstance(count, str) and count.lower() == 'auto':
        count = cpu_count()
    if not isinstance(count, int) or isinstance(count, bool) or count < 1:
        raise ConfigError(f'{where}: count must be a positive integer or auto, not {count!r}')

    min_symbols = raw.get('min_symbols', WorkersConfig.min_symbols)
    if not isinstance(min_symbols, int) or isinstance(min_symbols, bool) or min_symbols < 1:
        raise ConfigError(f'{where}: min_symbols must be a positive integer, not {min_symbols!r}')

    lag_warn = raw.get('lag_warn', WorkersConfig.lag_warn)
    if not isinstance(lag_warn, (int, float)) or isinstance(lag_warn, bool) or lag_warn <= 0:
        raise ConfigError(f'{where}: lag_warn must be a number of seconds, not {lag_warn!r}')
    return WorkersConfig(count=count, min_symbols=min_symbols, lag_warn=float(lag_warn))


def _parse_record(raw, source: str):
    if raw is None or raw is False:
        return None
    where = f'{source}: record'
    if isinstance(raw, str):
        return RecordConfig(path=raw)
    _expect_mapping(raw, where)
    _check_keys(raw, RECORD_KEYS, where)
    path = raw.get('path')
    if not isinstance(path, str) or not path:
        raise ConfigError(f'{where}: "path" (a pcap file or a directory) is required')
    rotate = raw.get('rotate_size')
    if rotate is not None and (not isinstance(rotate, int) or isinstance(rotate, bool) or rotate <= 0):
        raise ConfigError(f'{where}: rotate_size must be a positive number of bytes')
    compress = raw.get('compress', True)
    if not isinstance(compress, bool):
        raise ConfigError(f'{where}: compress must be true or false')
    return RecordConfig(path=path, rotate_size=rotate, compress=compress)


def _parse_log(raw, source: str) -> tuple:
    if raw is None:
        return 'INFO', None
    where = f'{source}: log'
    _expect_mapping(raw, where)
    _check_keys(raw, LOG_KEYS, where)
    level = str(raw.get('level', 'INFO')).upper()
    if level not in LOG_LEVELS:
        raise ConfigError(f'{where}: level must be one of {", ".join(LOG_LEVELS)}, not {raw.get("level")!r}')
    file = raw.get('file')
    if file is not None and not isinstance(file, str):
        raise ConfigError(f'{where}: file must be a path')
    return level, file


def _expand_env(node, source: str):
    if isinstance(node, dict):
        return {key: _expand_env(value, source) for key, value in node.items()}
    if isinstance(node, list):
        return [_expand_env(value, source) for value in node]
    if not isinstance(node, str) or '${' not in node:
        return node

    def substitute(match):
        name, default = match.group(1), match.group(2)
        value = os.environ.get(name)
        if value is None:
            if default is None:
                raise ConfigError(f'{source}: environment variable {name} is referenced but not set')
            return default
        return value

    expanded = _ENV_REF.sub(substitute, node)
    whole = _ENV_REF.fullmatch(node)
    if whole is not None:
        # the entire value came from the environment, so give numbers and booleans their types back
        try:
            return yaml.safe_load(expanded)
        except yaml.YAMLError:
            return expanded
    return expanded


def _expect_mapping(value, where: str):
    if not isinstance(value, dict):
        raise ConfigError(f'{where}: expected a mapping of key: value pairs, not {type(value).__name__}')


def _check_keys(mapping: dict, allowed, where: str):
    for key in mapping:
        if key not in allowed:
            raise ConfigError(f'{where}: unknown key {key!r}{_hint(str(key), allowed)} - valid keys are {", ".join(allowed)}')


def _hint(name: str, candidates) -> str:
    close = get_close_matches(name, list(candidates), n=1)
    return f" - did you mean '{close[0]}'?" if close else ''
