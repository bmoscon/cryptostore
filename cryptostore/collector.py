'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from multiprocessing import Process

from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES, L2_BOOK, L3_BOOK
from cryptofeed.backends.redis import TradeStream, BookStream


class Collector(Process):
    def __init__(self, exchange, exchange_config, config):
        self.exchange = exchange
        self.exchange_config = exchange_config
        self.config = config
        super().__init__()

    def run(self):
        fh = FeedHandler()
        cb = {}
        depth = None

        if 'book_depth' in self.config:
            depth = self.config['book_depth']

        if TRADES in self.exchange_config:
            cb[TRADES] = TradeStream(host=self.config['redis']['ip'], port=self.config['redis']['port'])
        if L2_BOOK in self.exchange_config:
            cb[L2_BOOK] = BookStream(depth=depth, key=L2_BOOK, host=self.config['redis']['ip'], port=self.config['redis']['port'])
        if L3_BOOK in self.exchange_config:
            cb[L3_BOOK] = BookStream(depth=depth, key=L3_BOOK, host=self.config['redis']['ip'], port=self.config['redis']['port'])

        fh.add_feed(self.exchange, config=self.exchange_config, callbacks=cb)
        fh.run()
