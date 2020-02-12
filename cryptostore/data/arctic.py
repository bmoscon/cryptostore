'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import pandas as pd
from cryptofeed.defines import TRADES, L2_BOOK, L3_BOOK, TICKER, FUNDING

from cryptostore.data.store import Store
from cryptostore.engines import StorageEngines


class Arctic(Store):
    def __init__(self, connection: str):
        self.data = []
        self.con = StorageEngines.arctic.Arctic(connection)

    def aggregate(self, data):
        self.data = data

    def write(self, exchange, data_type, pair, timestamp):
        chunk_size = None
        if not self.data:
            return
        df = pd.DataFrame(self.data)
        self.data = []

        if data_type == TRADES:
            if 'id' in df:
                df['id'] = df['id'].astype(str)
            df['size'] = df.amount
            df['date'] = pd.to_datetime(df['timestamp'], unit='s')
            df = df.drop(['pair', 'feed', 'amount'], axis=1)
            chunk_size = 'H'
        elif data_type == TICKER:
            df['date'] = pd.to_datetime(df['timestamp'], unit='s')
            df = df.drop(['pair', 'feed'], axis=1)
            chunk_size = 'D'
        elif data_type in { L2_BOOK, L3_BOOK }:
            df['date'] = pd.to_datetime(df['timestamp'], unit='s')
            chunk_size = 'T'
        elif data_type == FUNDING:
            df['date'] = pd.to_datetime(df['timestamp'], unit='s')
            chunk_size = 'D'

        df.set_index('date', inplace=True)
        df = df.drop(['timestamp'], axis=1)
        # All timestamps are in UTC
        df.index = df.index.tz_localize(None)
        if exchange not in self.con.list_libraries():
            self.con.initialize_library(exchange, lib_type=StorageEngines.arctic.CHUNK_STORE)
        self.con[exchange].append(f"{data_type}-{pair}", df, upsert=True, chunk_size=chunk_size)

    def get_start_date(self, exchange: str, data_type: str, pair: str) -> float:
        try:
            return next(self.con[exchange].iterator(f"{data_type}-{pair}")).index[0].timestamp()
        except Exception:
            return None
