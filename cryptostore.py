'''
Copyright (C) 2018-2024  Bryant Moscon - bmoscon@gmail.com
Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from datetime import datetime
import os

from cryptofeed import FeedHandler
from cryptofeed.exchanges import EXCHANGE_MAP
from cryptofeed.feed import Feed
from cryptofeed.defines import L2_BOOK, TICKER, TRADES, FUNDING, CANDLES, OPEN_INTEREST, LIQUIDATIONS
from cryptofeed.backends.redis import BookRedis, TradeRedis, TickerRedis, FundingRedis, CandlesRedis, OpenInterestRedis, LiquidationsRedis
from cryptofeed.backends.redis import BookStream, TradeStream, TickerStream, FundingStream, CandlesStream, OpenInterestStream, LiquidationsStream
from cryptofeed.backends.mongo import BookMongo, TradeMongo, TickerMongo, FundingMongo, CandlesMongo, OpenInterestMongo, LiquidationsMongo
from cryptofeed.backends.postgres import BookPostgres, TradePostgres, TickerPostgres, FundingPostgres, CandlesPostgres, OpenInterestPostgres, LiquidationsPostgres
from cryptofeed.backends.socket import BookSocket, TradeSocket, TickerSocket, FundingSocket, CandlesSocket, OpenInterestSocket, LiquidationsSocket
from cryptofeed.backends.influxdb import BookInflux, TradeInflux, TickerInflux, FundingInflux, CandlesInflux, OpenInterestInflux, LiquidationsInflux
from cryptofeed.backends.quest import BookQuest, TradeQuest, TickerQuest, FundingQuest, CandlesQuest, OpenInterestQuest, LiquidationsQuest


async def tty(obj, receipt_ts):
    # For debugging purposes
    rts = datetime.utcfromtimestamp(receipt_ts).strftime('%Y-%m-%d %H:%M:%S')
    print(f"{rts} - {obj}")


def load_config() -> Feed:
    exchange = os.environ.get('EXCHANGE')
    symbols = os.environ.get('SYMBOLS')

    if symbols is None:
        raise ValueError("Symbols must be specified")
    symbols = symbols.split(",")

    channels = os.environ.get('CHANNELS')
    if channels is None:
        raise ValueError("Channels must be specified")
    channels = channels.split(",")

    config = os.environ.get('CONFIG')
    backend = os.environ.get('BACKEND')
    snap_only = os.environ.get('SNAPSHOT_ONLY', False)
    if snap_only:
        if snap_only.lower().startswith('f'):
            snap_only = False
        elif snap_only.lower().startswith('t'):
            snap_only = True
        else:
            raise ValueError('Invalid value specified for SNAPSHOT_ONLY')
    snap_interval = os.environ.get('SNAPSHOT_INTERVAL', 1000)
    snap_interval = int(snap_interval)
    host = os.environ.get('HOST', '127.0.0.1')
    port = os.environ.get('PORT')
    if port:
        port = int(port)
    candle_interval = os.environ.get('CANDLE_INTERVAL', '1m')
    database = os.environ.get('DATABASE')
    user = os.environ.get('USER')
    password = os.environ.get('PASSWORD')
    org = os.environ.get('ORG')
    bucket = os.environ.get('BUCKET')
    token = os.environ.get('TOKEN')

    cbs = None
    if backend == 'REDIS' or backend == 'REDISSTREAM':
        kwargs = {'host': host, 'port': port if port else 6379}
        cbs = {
            L2_BOOK: BookRedis(snapshot_interval=snap_interval, snapshots_only=snap_only, **kwargs) if backend == 'REDIS' else BookStream(snapshot_interval=snap_interval, snapshots_only=snap_only, **kwargs),
            TRADES: TradeRedis(**kwargs) if backend == 'REDIS' else TradeStream(**kwargs),
            TICKER: TickerRedis(**kwargs) if backend == 'REDIS' else TickerStream(**kwargs),
            FUNDING: FundingRedis(**kwargs) if backend == 'REDIS' else FundingStream(**kwargs),
            CANDLES: CandlesRedis(**kwargs) if backend == 'REDIS' else CandlesStream(**kwargs),
            OPEN_INTEREST: OpenInterestRedis(**kwargs) if backend == 'REDIS' else OpenInterestStream(**kwargs),
            LIQUIDATIONS: LiquidationsRedis(**kwargs) if backend == 'REDIS' else LiquidationsStream(**kwargs)
        }
    elif backend == 'MONGO':
        kwargs = {'host': host, 'port': port if port else 27101}
        cbs = {
            L2_BOOK: BookMongo(database, snapshot_interval=snap_interval, snapshots_only=snap_only, **kwargs),
            TRADES: TradeMongo(database, **kwargs),
            TICKER: TickerMongo(database, **kwargs),
            FUNDING: FundingMongo(database, **kwargs),
            CANDLES: CandlesMongo(database, **kwargs),
            OPEN_INTEREST: OpenInterestMongo(database, **kwargs),
            LIQUIDATIONS: LiquidationsMongo(database, **kwargs)
        }
    elif backend == 'POSTGRES':
        kwargs = {'db': database, 'host': host, 'port': port if port else 5432, 'user': user, 'pw': password}
        cbs = {
            L2_BOOK: BookPostgres(snapshot_interval=snap_interval, snapshots_only=snap_only, **kwargs),
            TRADES: TradePostgres(**kwargs),
            TICKER: TickerPostgres(**kwargs),
            FUNDING: FundingPostgres(**kwargs),
            CANDLES: CandlesPostgres(**kwargs),
            OPEN_INTEREST: OpenInterestPostgres(**kwargs),
            LIQUIDATIONS: LiquidationsPostgres(**kwargs)
        }
    elif backend in ('TCP', 'UDP', 'UDS'):
        kwargs = {'port': port}
        cbs = {
            L2_BOOK: BookSocket(host, snapshot_interval=snap_interval, snapshots_only=snap_only, **kwargs),
            TRADES: TradeSocket(host, **kwargs),
            TICKER: TickerSocket(host, **kwargs),
            FUNDING: FundingSocket(host, **kwargs),
            CANDLES: CandlesSocket(host, **kwargs),
            OPEN_INTEREST: OpenInterestSocket(host, **kwargs),
            LIQUIDATIONS: LiquidationsSocket(host, **kwargs)
        }
    elif backend == 'INFLUX':
        args = (host, org, bucket, token)
        cbs = {
            L2_BOOK: BookInflux(*args, snapshot_interval=snap_interval, snapshots_only=snap_only),
            TRADES: TradeInflux(*args),
            TICKER: TickerInflux(*args),
            FUNDING: FundingInflux(*args),
            CANDLES: CandlesInflux(*args),
            OPEN_INTEREST: OpenInterestInflux(*args),
            LIQUIDATIONS: LiquidationsInflux(*args)
        }
    elif backend == 'QUEST':
        kwargs = {'host': host, 'port': port if port else 9009}
        cbs = {
            L2_BOOK: BookQuest(**kwargs),
            TRADES: TradeQuest(**kwargs),
            TICKER: TickerQuest(**kwargs),
            FUNDING: FundingQuest(**kwargs),
            CANDLES: CandlesQuest(**kwargs),
            OPEN_INTEREST: OpenInterestQuest(**kwargs),
            LIQUIDATIONS: LiquidationsQuest(**kwargs)
        }
    elif backend == 'TTY':
        cbs = {
            L2_BOOK: tty,
            TRADES: tty,
            TICKER: tty,
            FUNDING: tty,
            CANDLES: tty,
            OPEN_INTEREST: tty,
            LIQUIDATIONS: tty
        }
    else:
        raise ValueError('Invalid backend specified')

    # Prune unused callbacks
    remove = [chan for chan in cbs if chan not in channels]
    for r in remove:
        del cbs[r]

    return EXCHANGE_MAP[exchange](candle_interval=candle_interval, symbols=symbols, channels=channels, config=config, callbacks=cbs)


def main():
    fh = FeedHandler()
    cfg = load_config()
    fh.add_feed(cfg)
    fh.run()


if __name__ == '__main__':
    main()
