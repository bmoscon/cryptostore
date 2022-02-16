'''
Copyright (C) 2018-2022  Bryant Moscon - bmoscon@gmail.com
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
from cryptofeed.backends.mongo import BookMongo, TradeMongo, TickerMongo, FundingMongo, CandlesMongo, OpenInterestMongo, LiquidationsMongo


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
    snap_interval = os.environ.get('SNAPSHOT_INTERVAL', 1000)
    host = os.environ.get('host', '127.0.0.1')
    port = os.environ.get('port')
    candle_interval = os.environ.get('CANDLE_INTERVAL')
    database = os.environ.get('DATABASE')

    cbs = None
    if backend == 'REDIS':
        cbs = {
            L2_BOOK: BookRedis(host=host, port=port if port else 6379, snapshot_interval=snap_interval, snapshots_only=snap_only),
            TRADES: TradeRedis(host=host, port=port if port else 6379),
            TICKER: TickerRedis(host=host, port=port if port else 6379),
            FUNDING: FundingRedis(host=host, port=port if port else 6379),
            CANDLES: CandlesRedis(host=host, port=port if port else 6379),
            OPEN_INTEREST: OpenInterestRedis(host=host, port=port if port else 6379),
            LIQUIDATIONS: LiquidationsRedis(host=host, port=port if port else 6379)
        }
    elif backend == 'MONGO':
        cbs = {
            L2_BOOK: BookMongo(database, host=host, port=port if port else 27101, snapshot_interval=snap_interval, snapshots_only=snap_only),
            TRADES: TradeMongo(database, host=host, port=port if port else 27101),
            TICKER: TickerMongo(database, host=host, port=port if port else 27101),
            FUNDING: FundingMongo(database, host=host, port=port if port else 27101),
            CANDLES: CandlesMongo(database, host=host, port=port if port else 27101),
            OPEN_INTEREST: OpenInterestMongo(database, host=host, port=port if port else 27101),
            LIQUIDATIONS: LiquidationsMongo(database, host=host, port=port if port else 27101)
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

    return EXCHANGE_MAP[exchange](candle_intterval=candle_interval, symbols=symbols, channels=channels, config=config, callbacks=cbs)


def main():
    cfg = load_config()

    fh = FeedHandler()
    fh.add_feed(cfg)
    fh.run()


if __name__ == '__main__':
    main()
