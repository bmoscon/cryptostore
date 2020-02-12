'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import os
from multiprocessing import Process
import logging

from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES, L2_BOOK, L3_BOOK, BOOK_DELTA, TICKER, FUNDING


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
        fh = FeedHandler(retries=retries)


        for callback_type, value in self.exchange_config.items():
            cb = {}
            depth = None
            window = 1000
            delta = False

            if 'book_interval' in value:
                window = value['book_interval']
            if 'book_delta' in value and value['book_delta']:
                delta = True
            if 'max_depth' in value:
                depth = value['max_depth']

            if callback_type in (L2_BOOK, L3_BOOK):
                self.exchange_config[callback_type] = self.exchange_config[callback_type]['symbols']

            if cache == 'redis':
                if 'ip' in self.config['redis'] and self.config['redis']['ip']:
                    kwargs = {'host': self.config['redis']['ip'], 'port': self.config['redis']['port'], 'numeric_type': float}
                else:
                    kwargs = {'socket': self.config['redis']['socket'], 'numeric_type': float}
                from cryptofeed.backends.redis import TradeStream, BookStream, BookDeltaStream, TickerStream, FundingStream
                trade_cb = TradeStream
                book_cb = BookStream
                book_up = BookDeltaStream if delta else None
                ticker_cb = TickerStream
                funding_cb = FundingStream
            elif cache == 'kafka':
                from cryptofeed.backends.kafka import TradeKafka, BookKafka, BookDeltaKafka, TickerKafka, FundingKafka
                trade_cb = TradeKafka
                book_cb = BookKafka
                book_up = BookDeltaKafka if delta else None
                ticker_cb = TickerKafka
                funding_cb = FundingKafka
                kwargs = {'host': self.config['kafka']['ip'], 'port': self.config['kafka']['port']}

            if callback_type == TRADES:
                cb[TRADES] = [trade_cb(**kwargs)]
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


            if 'pass_through' in self.config:
                if self.config['pass_through']['type'] == 'zmq':
                    from cryptofeed.backends.zmq import TradeZMQ, BookDeltaZMQ, BookZMQ, FundingZMQ
                    import zmq
                    host = self.config['pass_through']['host']
                    port = self.config['pass_through']['port']

                    if callback_type == TRADES:
                        cb[TRADES].append(TradeZMQ(host=host, port=port, zmq_type=zmq.PUB))
                    elif callback_type == FUNDING:
                        cb[FUNDING].append(FundingZMQ(host=host, port=port, zmq_type=zmq.PUB))
                    elif callback_type == L2_BOOK:
                        cb[L2_BOOK].append(BookZMQ(host=host, port=port, zmq_type=zmq.PUB))
                    elif callback_type == L3_BOOK:
                        cb[L3_BOOK].append(BookZMQ(host=host, port=port, zmq_type=zmq.PUB))
                    if BOOK_DELTA in cb:
                        cb[BOOK_DELTA].append(BookDeltaZMQ(host=host, port=port, zmq_type=zmq.PUB))

            fh.add_feed(self.exchange, max_depth=depth, book_interval=window, config={callback_type: self.exchange_config[callback_type]}, callbacks=cb)

        fh.run()
