'''
Copyright (C) 2018-2026 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.


Symbol patterns and the worker plan.

A channel's symbol list may contain `all` or glob patterns (`*-USDT`, `BTC-*`); resolve_symbols()
expands them against the exchange's symbol list once it has been downloaded.

plan_workers() splits the resolved subscriptions across the configured number of workers. Every
exchange with enough symbols is sharded: shard i takes every n-th (channel, symbol) pair, so each
shard carries the same channels and roughly the same load. Shards are then dealt to the workers
largest first, each to the least loaded worker. One worker is one FeedHandler on its own thread
and event loop, so the plan decides how the work spreads across cores.
'''
import fnmatch
from dataclasses import dataclass, field, replace

from cryptostore.config import ALL_SYMBOLS, ChannelConfig, Config, ConfigError, ExchangeConfig, is_pattern


def expand_symbols(channel: ChannelConfig, known: list, exchange: str) -> ChannelConfig:
    '''replace `all` and glob patterns with the matching symbols from `known`, keeping the explicit ones'''
    symbols = []
    patterns = []
    for symbol in channel.symbols:
        if not is_pattern(symbol):
            symbols.append(symbol)
            continue
        patterns.append(symbol)
        matched = list(known) if symbol.lower() == ALL_SYMBOLS else fnmatch.filter(known, symbol)
        if not matched:
            raise ConfigError(f'{exchange} {channel.channel}: {symbol!r} matches none of the {len(known)} symbols on {exchange}')
        symbols.extend(sorted(matched))
    return replace(channel, symbols=tuple(dict.fromkeys(symbols)), patterns=tuple(patterns))


def resolve_symbols(config: Config, known: dict) -> Config:
    '''
    known: exchange id -> its symbol list, for every exchange whose channels use a pattern.
    Returns a Config with every pattern expanded; exchanges without patterns are untouched
    '''
    exchanges = []
    for exchange in config.exchanges:
        if not any(channel.patterns for channel in exchange.channels):
            exchanges.append(exchange)
            continue
        if exchange.name not in known:
            raise ConfigError(f'{exchange.name}: the symbol list is needed to expand {", ".join(p for c in exchange.channels for p in c.patterns)}')
        channels = tuple(expand_symbols(channel, known[exchange.name], exchange.name) for channel in exchange.channels)
        exchanges.append(replace(exchange, channels=channels))
    return replace(config, exchanges=tuple(exchanges))


@dataclass(frozen=True)
class Shard:
    exchange: ExchangeConfig   # the exchange with this shard's slice of the symbols
    index: int                 # 0-based
    count: int                 # how many shards the exchange was split into

    @property
    def label(self) -> str:
        return self.exchange.name if self.count == 1 else f'{self.exchange.name}[{self.index + 1}/{self.count}]'

    @property
    def symbols(self) -> int:
        return sum(len(channel.symbols) for channel in self.exchange.channels)


@dataclass
class WorkerPlan:
    index: int
    shards: list = field(default_factory=list)

    @property
    def symbols(self) -> int:
        return sum(shard.symbols for shard in self.shards)

    @property
    def labels(self) -> list:
        return [shard.label for shard in self.shards]


def split_exchange(exchange: ExchangeConfig, count: int) -> list:
    '''deal the exchange's (channel, symbol) pairs round-robin into `count` shards'''
    if count <= 1:
        return [Shard(exchange, 0, 1)]
    pairs = [(channel, symbol) for channel in exchange.channels for symbol in channel.symbols]
    count = min(count, len(pairs))
    shards = []
    for i in range(count):
        mine = pairs[i::count]
        channels = []
        for channel in exchange.channels:
            symbols = tuple(symbol for owner, symbol in mine if owner is channel)
            if symbols:
                channels.append(replace(channel, symbols=symbols))
        shards.append(Shard(replace(exchange, channels=tuple(channels)), i, count))
    return shards


def shard_count(exchange: ExchangeConfig, workers: int, min_symbols: int) -> int:
    '''how many shards an exchange is split into: never more than the workers, never fewer than min_symbols symbols each'''
    if workers <= 1:
        return 1
    total = sum(len(channel.symbols) for channel in exchange.channels)
    count = min(workers, total // max(1, min_symbols))
    if exchange.shards is not None:
        count = min(count, exchange.shards)
    return max(1, count)


def plan_workers(config: Config) -> list:
    '''
    Splits the (resolved) config into workers. Workers that would get nothing are dropped, so a small
    config on an 8-worker setting runs fewer threads
    '''
    count, min_symbols = config.workers.count, config.workers.min_symbols
    shards = []
    for exchange in config.exchanges:
        shards.extend(split_exchange(exchange, shard_count(exchange, count, min_symbols)))

    workers = [WorkerPlan(i) for i in range(count)]
    for shard in sorted(shards, key=lambda shard: (-shard.symbols, shard.exchange.name, shard.index)):
        target = min(workers, key=lambda worker: (worker.symbols, worker.index))
        target.shards.append(shard)

    used = [worker for worker in workers if worker.shards]
    for i, worker in enumerate(used):
        worker.index = i
    return used
