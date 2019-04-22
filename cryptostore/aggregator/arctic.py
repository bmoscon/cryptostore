'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import pandas as pd
from arctic import Arctic as ar
from arctic import CHUNK_STORE
from cryptofeed.defines import TRADES

from cryptostore.aggregator.store import Store


class Arctic(Store):
    def __init__(self, connection: str):
        self.data = []
        self.con = ar(connection)

    def aggregate(self, data):
        self.data.extend(data)

    def write(self, exchange, data_type, pair, timestamp):
        chunk_size = None
        df = pd.DataFrame(self.data)
        self.data = []
        if data_type == TRADES:
            df['amount'] = df.amount.astype('float')
            df['size'] = df.amount.astype('float')
            df['date'] = pd.to_datetime(df['timestamp'])
            df = df.drop(['pair', 'feed', 'timestamp'], axis=1)
            df.set_index('date', inplace=True)
            # All timestamps are in UTC
            df.index = df.index.tz_localize(None)
            chunk_size = 'H'

        if exchange not in self.con.list_libraries():
            self.con.initialize_library(exchange, lib_type=CHUNK_STORE)
        self.con[exchange].append(f"{data_type}-{pair}", df, upsert=True, chunk_size=chunk_size)
