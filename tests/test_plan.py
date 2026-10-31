'''
Copyright (C) 2018-2026 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import pytest
from cryptofeed.defines import L2_BOOK, TRADES

from cryptostore.config import ConfigError, is_pattern, parse
from cryptostore.plan import plan_workers, resolve_symbols, shard_count, split_exchange


def make_config(channels: dict, workers: int = 1, min_symbols: int = 50, shards: int = None, exchange: str = 'BINANCE', **extra_exchanges):
    raw = {
        'backends': {'out': {'type': 'tty'}},
        'exchanges': {exchange: {'channels': channels}},
        'workers': {'count': workers, 'min_symbols': min_symbols},
    }
    if shards is not None:
        raw['exchanges'][exchange]['shards'] = shards
    for name, chans in extra_exchanges.items():
        raw['exchanges'][name] = {'channels': chans}
    return parse(raw)


def symbols(n, prefix='S'):
    return [f'{prefix}{i}-USDT' for i in range(n)]


def test_is_pattern():
    assert is_pattern('all') and is_pattern('ALL') and is_pattern('*-USDT') and is_pattern('BTC-?SD') and is_pattern('[AB]TC-USD')
    assert not is_pattern('BTC-USD') and not is_pattern('BTC-USDT-PERP') and not is_pattern('allx')


def test_resolve_symbols_expands_patterns_and_keeps_the_rest():
    config = make_config({'trades': ['*-USD', 'BTC-EUR'], 'l2_book': 'all'}, exchange='COINBASE', KRAKEN={'trades': ['BTC-USD']})
    assert config.exchanges_with_patterns() == ['COINBASE']
    known = {'COINBASE': ['ETH-USD', 'BTC-USD', 'BTC-EUR', 'DOGE-BTC']}
    resolved = resolve_symbols(config, known)

    coinbase, kraken = resolved.exchanges
    trades, book = coinbase.channels
    assert trades.symbols == ('BTC-USD', 'ETH-USD', 'BTC-EUR')          # pattern matches sorted, explicit kept
    assert trades.patterns == ('*-USD',)
    assert book.symbols == ('BTC-EUR', 'BTC-USD', 'DOGE-BTC', 'ETH-USD') and book.patterns == ('all',)
    assert kraken is config.exchanges[1]                                 # untouched
    assert resolved.exchanges_with_patterns() == ['COINBASE']           # still remembers where the symbols came from
    assert config.exchanges[0].channels[0].symbols == ('*-USD', 'BTC-EUR')   # the original is not mutated


def test_resolve_symbols_errors():
    config = make_config({'trades': ['*-JPY']}, exchange='COINBASE')
    with pytest.raises(ConfigError, match='matches none of the 2 symbols'):
        resolve_symbols(config, {'COINBASE': ['BTC-USD', 'ETH-USD']})
    with pytest.raises(ConfigError, match='symbol list is needed'):
        resolve_symbols(config, {})


def test_single_worker_is_one_unsplit_shard():
    config = make_config({'l2_book': symbols(1000), 'trades': symbols(2)}, workers=1)
    plan = plan_workers(config)
    assert len(plan) == 1 and plan[0].labels == ['BINANCE'] and plan[0].symbols == 1002
    assert plan[0].shards[0].exchange.subscription == config.exchanges[0].subscription


def test_exchange_is_sharded_across_workers():
    config = make_config({'l2_book': symbols(1000), 'trades': symbols(2)}, workers=4)
    plan = plan_workers(config)
    assert [worker.labels for worker in plan] == [['BINANCE[1/4]'], ['BINANCE[2/4]'], ['BINANCE[3/4]'], ['BINANCE[4/4]']]
    shards = [worker.shards[0] for worker in plan]
    books = [shard.exchange.subscription[L2_BOOK] for shard in shards]
    assert [len(book) for book in books] == [250, 250, 250, 250]
    assert sorted(sum(books, [])) == sorted(symbols(1000))               # every symbol exactly once
    assert all(L2_BOOK in shard.exchange.subscription for shard in shards)
    assert shards[0].exchange.subscription[TRADES] == ['S0-USDT'] and shards[1].exchange.subscription[TRADES] == ['S1-USDT']
    assert TRADES not in shards[2].exchange.subscription               # a channel with nothing left is dropped from that shard
    assert all(shard.exchange.options == config.exchanges[0].options for shard in shards)


def test_small_exchanges_are_spread_not_split():
    config = make_config({'trades': symbols(5)}, workers=2, exchange='COINBASE', KRAKEN={'trades': symbols(5)}, BITFINEX={'trades': symbols(5)})
    plan = plan_workers(config)
    assert len(plan) == 2
    assert plan[0].labels == ['BITFINEX', 'KRAKEN'] and plan[1].labels == ['COINBASE']   # largest first, then least loaded
    assert all(shard.count == 1 for worker in plan for shard in worker.shards)


def test_min_symbols_limits_the_shards():
    config = make_config({'trades': symbols(120)}, workers=8, min_symbols=50)
    plan = plan_workers(config)
    assert len(plan) == 2 and [worker.symbols for worker in plan] == [60, 60]
    assert shard_count(config.exchanges[0], workers=8, min_symbols=200) == 1
    assert shard_count(config.exchanges[0], workers=8, min_symbols=1) == 8


def test_exchange_shards_cap():
    config = make_config({'trades': symbols(1000)}, workers=8, shards=3)
    plan = plan_workers(config)
    assert len(plan) == 3 and plan[0].labels == ['BINANCE[1/3]']


def test_idle_workers_are_dropped_and_renumbered():
    config = make_config({'trades': symbols(3)}, workers=8)
    plan = plan_workers(config)
    assert len(plan) == 1 and plan[0].index == 0 and plan[0].labels == ['BINANCE']


def test_split_never_leaves_an_empty_shard():
    config = make_config({'trades': symbols(5), 'l2_book': symbols(5, 'B')})
    shards = split_exchange(config.exchanges[0], 10)
    assert len(shards) == 10 and all(shard.symbols == 1 for shard in shards)
    assert all(len(shard.exchange.channels) == 1 for shard in shards)
    shards = split_exchange(config.exchanges[0], 50)
    assert len(shards) == 10                                             # never more shards than symbols
