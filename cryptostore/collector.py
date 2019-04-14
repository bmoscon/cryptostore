'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from multiprocessing import Process

from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES
from cryptofeed.backends.redis import TradeStream


class Collector(Process):
    def __init__(self, exchange, exchange_config, config):
        self.exchange = exchange
        self.exchange_config = exchange_config
        self.config = config
        super().__init__()

    def run(self):
        fh = FeedHandler()
        cb = {TRADES: TradeStream(host=self.config['redis']['ip'], port=self.config['redis']['port'])}
        fh.add_feed(self.exchange, config=self.exchange_config, callbacks=cb)
        fh.run()
