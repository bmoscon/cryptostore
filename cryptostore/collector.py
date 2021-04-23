'''
Copyright (C) 2018-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import os
from multiprocessing import Process
import logging

from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES, L2_BOOK, L3_BOOK, BOOK_DELTA, TICKER, FUNDING, OPEN_INTEREST, LIQUIDATIONS, CANDLES


LOG = logging.getLogger('cryptostore')


class Collector(Process):
    def __init__(self, exchange, exchange_config, config):
        self.exchange = exchange
        self.exchange_config = exchange_config
        self.config = config
        super().__init__()
        self.daemon = True

    def run(self):
        LOG.info("Collector for %s running on PID %d", self.exchange, os.getpid())

        cache = self.config['cache']
        retries = self.exchange_config.pop('retries', 30)
        timeouts = self.exchange_config.pop('channel_timeouts', {})
        fh = FeedHandler()

        for callback_type, value in self.exchange_config.items():
            cb = {}
            delta = False
            fh_kwargs = {'timeout': timeouts.get(callback_type, 120), 'book_interval': 1000}

            if 'book_interval' in value:
                fh_kwargs['book_interval'] = value['book_interval']
            if 'book_delta' in value and value['book_delta']:
                delta = True
            if 'max_depth' in value:
                fh_kwargs['max_depth'] = value['max_depth']
            if 'snapshot_interval' in value:
                fh_kwargs['snapshot_interval'] = value['snapshot_interval']

            if callback_type in (L2_BOOK, L3_BOOK):
                self.exchange_config[callback_type] = self.exchange_config[callback_type]['symbols']

            if cache == 'redis':
                if 'ip' in self.config['redis'] and self.config['redis']['ip']:
                    kwargs = {'host': self.config['redis']['ip'], 'port': self.config['redis']['port'], 'numeric_type': float}
                else:
                    kwargs = {'socket': self.config['redis']['socket'], 'numeric_type': float}
                from cryptofeed.backends.redis import TradeStream, BookStream, BookDeltaStream, TickerStream, FundingStream, OpenInterestStream, LiquidationsStream, CandlesStream
                trade_cb = TradeStream
                book_cb = BookStream
                book_up = BookDeltaStream if delta else None
                ticker_cb = TickerStream
                funding_cb = FundingStream
                oi_cb = OpenInterestStream
                liq_cb = LiquidationsStream
                candles_cb = CandlesStream
            elif cache == 'kafka':
                from cryptofeed.backends.kafka import TradeKafka, BookKafka, BookDeltaKafka, TickerKafka, FundingKafka, OpenInterestKafka, LiquidationsKafka, CandlesKafka
                trade_cb = TradeKafka
                book_cb = BookKafka
                book_up = BookDeltaKafka if delta else None
                ticker_cb = TickerKafka
                funding_cb = FundingKafka
                oi_cb = OpenInterestKafka
                liq_cb = LiquidationsKafka
                candles_cb = CandlesKafka
                kwargs = {'bootstrap': self.config['kafka']['ip'], 'port': self.config['kafka']['port']}

            if callback_type == TRADES:
                cb[TRADES] = [trade_cb(**kwargs)]
            elif callback_type == LIQUIDATIONS:
                cb[LIQUIDATIONS] = [liq_cb(**kwargs)]
            elif callback_type == FUNDING:
                cb[FUNDING] = [funding_cb(**kwargs)]
            elif callback_type == TICKER:
                cb[TICKER] = [ticker_cb(**kwargs)]
            elif callback_type == L2_BOOK:
                cb[L2_BOOK] = [book_cb(key=L2_BOOK, **kwargs)]
                if book_up:
                    cb[BOOK_DELTA] = [book_up(key=L2_BOOK, **kwargs)]
            elif callback_type == L3_BOOK:
                cb[L3_BOOK] = [book_cb(key=L3_BOOK, **kwargs)]
                if book_up:
                    cb[BOOK_DELTA] = [book_up(key=L3_BOOK, **kwargs)]
            elif callback_type == OPEN_INTEREST:
                cb[OPEN_INTEREST] = [oi_cb(**kwargs)]
            elif callback_type == CANDLES:
                cb[CANDLES] = [candles_cb(**kwargs)]

            if 'pass_through' in self.config:
                if self.config['pass_through']['type'] == 'zmq':
                    from cryptofeed.backends.zmq import TradeZMQ, BookDeltaZMQ, BookZMQ, FundingZMQ, OpenInterestZMQ, TickerZMQ, LiquidationsZMQ, CandlesZMQ
                    host = self.config['pass_through']['host']
                    port = self.config['pass_through']['port']

                    if callback_type == TRADES:
                        cb[TRADES].append(TradeZMQ(host=host, port=port))
                    elif callback_type == LIQUIDATIONS:
                        cb[LIQUIDATIONS].append(LiquidationsZMQ(host=host, port=port))
                    elif callback_type == FUNDING:
                        cb[FUNDING].append(FundingZMQ(host=host, port=port))
                    elif callback_type == L2_BOOK:
                        cb[L2_BOOK].append(BookZMQ(host=host, port=port))
                    elif callback_type == L3_BOOK:
                        cb[L3_BOOK].append(BookZMQ(host=host, port=port))
                    elif callback_type == OPEN_INTEREST:
                        cb[OPEN_INTEREST].append(OpenInterestZMQ(host=host, port=port))
                    elif callback_type == TICKER:
                        cb[TICKER].append(TickerZMQ(host=host, port=port))
                    elif callback_type == CANDLES:
                        cb[CANDLES].append(CandlesZMQ(host=host, port=port))
                    if BOOK_DELTA in cb:
                        cb[BOOK_DELTA].append(BookDeltaZMQ(host=host, port=port))

            fh.add_feed(self.exchange, retries=retries, subscription={callback_type: self.exchange_config[callback_type]}, callbacks=cb, **fh_kwargs)

        fh.run()
