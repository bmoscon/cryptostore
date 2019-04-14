'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from multiprocessing import Process

from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES
from cryptofeed.callback import TradeCallback


def trade(feed, pair, order_id, timestamp, side, amount, price):
    print(f"Timestamp: {timestamp} Feed: {feed} Pair: {pair} ID: {order_id} Side: {side} Amount: {amount} Price: {price}")


class Collector(Process):
    def __init__(self, exchange, config):
        self.exchange = exchange
        self.config = config
        super().__init__()

    
    def run(self):
        fh = FeedHandler()
        cb = {TRADES: TradeCallback(trade)}
        print(type(self.config))
        fh.add_feed(self.exchange, config=self.config, callbacks=cb)
        fh.run()
