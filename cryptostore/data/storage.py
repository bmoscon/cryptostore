'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from cryptostore.data.store import Store
from cryptostore.data.parquet import Parquet
from cryptostore.data.arctic import Arctic


class Storage(Store):
    def __init__(self, config):
        if isinstance(config.storage, list):
            self.s = [Storage.__init_helper(s, config) for s in config.storage]
        else:
            self.s = [Storage.__init_helper(config.storage, config)]

    @staticmethod
    def __init_helper(store, config):
        if store == 'parquet':
            return Parquet(config.parquet)
        elif store == 'arctic':
            return Arctic(config.arctic)
        else:
            raise ValueError("Store type not supported")

    def write(self, exchange, data_type, pair, timestamp):
        for s in self.s:
            s.write(exchange, data_type, pair, timestamp)

    def aggregate(self, data):
        for s in self.s:
            s.aggregate(data)
