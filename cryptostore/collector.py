'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import os
from multiprocessing import Process
import logging

from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES, L2_BOOK, L3_BOOK, BOOK_DELTA


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
        fh = FeedHandler()
        cb = {}
        depth = None

        if 'book_depth' in self.config:
            depth = self.config['book_depth']

        cache = self.config['cache']
        if cache == 'redis':
            from cryptofeed.backends.redis import TradeStream, BookStream, BookDeltaStream
            trade_cb = TradeStream
            book_cb = BookStream
            book_up = BookDeltaStream if not depth and self.config['book_delta'] else None
            kwargs = {'host': self.config['redis']['ip'], 'port': self.config['redis']['port']}
        elif cache == 'kafka':
            from cryptofeed.backends.kafka import TradeKafka, BookKafka, BookDeltaKafka
            trade_cb = TradeKafka
            book_cb = BookKafka
            book_up = BookDeltaKafka if not depth and self.config['book_delta'] else None
            kwargs = {'host': self.config['kafka']['ip'], 'port': self.config['kafka']['port']}

        if TRADES in self.exchange_config:
            cb[TRADES] = trade_cb(**kwargs)
        if L2_BOOK in self.exchange_config:
            cb[L2_BOOK] = book_cb(key=L2_BOOK, depth=depth, **kwargs)
            if book_up:
                cb[BOOK_DELTA] = book_up(key=L2_BOOK, **kwargs)
        if L3_BOOK in self.exchange_config:
            cb[L3_BOOK] = book_cb(key=L3_BOOK, depth=depth, **kwargs)
            if book_up:
                cb[BOOK_DELTA] = book_up(key=L3_BOOK, **kwargs)

        fh.add_feed(self.exchange, config=self.exchange_config, callbacks=cb)
        fh.run()
