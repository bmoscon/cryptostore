'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import pandas as pd
from cryptofeed.defines import TRADES, L2_BOOK, L3_BOOK, TICKER, FUNDING, OPEN_INTEREST

from cryptostore.data.store import Store
from cryptostore.engines import StorageEngines


class Arctic(Store):
    def __init__(self, connection: str):
        self.data = pd.DataFrame()
        self.con = StorageEngines.arctic.Arctic(connection)

    def aggregate(self, data):
        if isinstance(data[0], dict):
            # Case `data` is a list or tuple of dict.
            self.data = pd.DataFrame(data)
        else:
            # Case `data` is a tuple with tuple of keys of dict as 1st parameter,
            # and generator of dicts as 2nd paramter.
            # DataFrame creation is faster by more than 10% if column names are provided.
            self.data = pd.DataFrame(data[1], columns=data[0])

    def write(self, exchange, data_type, pair, timestamp):
        if self.data.empty:
            return

        df = self.data
        df['date'] = pd.to_datetime(df['timestamp'], unit='s')
        df['receipt_timestamp'] = pd.to_datetime(df['receipt_timestamp'], unit='s')
        df = df.drop(['timestamp'], axis=1)

        chunk_size = None
        if data_type == TRADES:
            if 'id' in df:
                df['id'] = df['id'].astype(str)
            df['size'] = df.amount
            df = df.drop(['pair', 'feed', 'amount'], axis=1)
            chunk_size = 'H'
        elif data_type == TICKER:
            df = df.drop(['pair', 'feed'], axis=1)
            chunk_size = 'D'
        elif data_type in { L2_BOOK, L3_BOOK }:
            chunk_size = 'T'
        elif data_type == FUNDING:
            chunk_size = 'D'
        elif data_type == OPEN_INTEREST:
            df = df.drop(['pair', 'feed'], axis=1)
            chunk_size = 'D'

        df.set_index('date', inplace=True)
        # All timestamps are in UTC
        df.index = df.index.tz_localize(None)
        if exchange not in self.con.list_libraries():
            self.con.initialize_library(exchange, lib_type=StorageEngines.arctic.CHUNK_STORE)
            # set the quota of each arctic library to unlimited
            self.con.set_quota(exchange, 0)
        self.con[exchange].append(f"{data_type}-{pair}", df, upsert=True, chunk_size=chunk_size)

    def get_start_date(self, exchange: str, data_type: str, pair: str) -> float:
        try:
            return next(self.con[exchange].iterator(f"{data_type}-{pair}")).index[0].timestamp()
        except Exception:
            return None
