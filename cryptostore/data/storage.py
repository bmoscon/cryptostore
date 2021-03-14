'''
Copyright (C) 2018-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from cryptostore.data.store import Store
from cryptostore.data.parquet import Parquet
from cryptostore.data.arctic import Arctic
from cryptostore.data.influx import InfluxDB
from cryptostore.data.elastic import ElasticSearch


class Storage(Store):
    def __init__(self, config, **kwargs):
        self.config = config
        if isinstance(config.storage, list):
            self.s = [Storage.__init_helper(s, config, **kwargs) for s in config.storage]
        else:
            self.s = [Storage.__init_helper(config.storage, config, **kwargs)]

    @staticmethod
    def __init_helper(store, config, **kwargs):
        if store == 'parquet':
            return Parquet(config.exchanges, config.parquet if 'parquet' in config else None, **kwargs)
        elif store == 'arctic':
            return Arctic(config.arctic)
        elif store == 'influx':
            return InfluxDB(config.influx)
        elif store == 'elastic':
            return ElasticSearch(config.elastic)
        else:
            raise ValueError("Store type not supported")

    def write(self, exchange, data_type: str, pair: str, timestamp: float):
        for s in self.s:
            s.write(exchange, data_type, pair, timestamp)

    def aggregate(self, data: list, transform=lambda x: x):
        for s in self.s:
            s.aggregate(transform(data))

    def get_start_date(self, exchange: str, data_type: str, pair: str) -> list:
        ret = []
        for s in self.s:
            ret.append(s.get_start_date(exchange, data_type, pair))
        return ret
