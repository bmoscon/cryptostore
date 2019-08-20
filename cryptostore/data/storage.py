"""
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
"""
from cryptostore.data.store import Store
from cryptostore.data.parquet import Parquet
from cryptostore.data.arctic import Arctic
from cryptostore.data.influx import InfluxDB
from cryptostore.data.elastic import ElasticSearch


class Storage(Store):
    """
    This class defines the various storage mediums supported by cryptostore.
    """

    def __init__(self, config):
        if "wide_tables" in config and config.wide_tables:
            if "arctic" not in config:
                raise Exception(
                    "[wide_tables] can only be enabled if arctic is one of the storage mediums."
                )

        # If there is more than one storage medium.
        if isinstance(config.storage, list):
            self.s = [Storage.__init_helper(s, config) for s in config.storage]
        # Or if there is only one.
        else:
            self.s = [Storage.__init_helper(config.storage, config)]

    @staticmethod
    def __init_helper(store, config):
        if store == "parquet":
            return Parquet(config.parquet if "parquet" in config else None)
        elif store == "arctic":
            if "wide_tables" in config and config.wide_tables:
                return Arctic(config.arctic, wide_tables=True)
            return Arctic(config.arctic)
        elif store == "influx":
            return InfluxDB(config.influx)
        elif store == "elastic":
            return ElasticSearch(config.elastic)
        else:
            raise ValueError("Store type not supported.")

    def write(self, exchange, data_type: str, pair: str, timestamp: float):
        for s in self.s:
            s.write(exchange, data_type, pair, timestamp)

    def aggregate(self, data: list):
        for s in self.s:
            s.aggregate(data)

    def get_start_date(self, exchange: str, data_type: str, pair: str) -> list:
        ret = []
        for s in self.s:
            ret.append(s.get_start_date(exchange, data_type, pair))
        return ret
