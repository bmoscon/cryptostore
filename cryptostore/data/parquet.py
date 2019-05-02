'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import os

import pyarrow as pa
import pyarrow.parquet as pq

from cryptostore.data.store import Store
from cryptostore.data.gc import google_cloud_write


class Parquet(Store):
    def __init__(self, config=None):
        self._write = None
        self.data = None

        if config:
            if 'GCS' in config:
                self._write = google_cloud_write
                self.bucket = config['GCS']['bucket']
                self.prefix = config['GCS']['prefix']
                self.auth = config['GCS']['service_account']
                self.del_file = False if 'del_file' in config['GCS'] and config['GCS']['del_file'] == False else True

    def aggregate(self, data):
        names = list(data[0].keys())
        cols = {name : [] for name in names}

        for entry in data:
            for key in entry:
                cols[key].append(entry[key])
        arrays = [pa.array(cols[col]) for col in cols]
        table = pa.Table.from_arrays(arrays, names=names)

        if self.data is None:
            self.data = table
        else:
            self.data = pa.concat_tables(self.data, table)

    def write(self, exchange, data_type, pair, timestamp):
        file_name = f'{exchange}-{data_type}-{pair}-{int(timestamp)}.parquet'
        pq.write_table(self.data, file_name)
        self.data = None

        if self._write:
            path = f'{exchange}/{data_type}/{pair}/{int(timestamp)}.parquet'
            if self.prefix:
                path = f"{self.prefix}/{path}"
            self._write(self.bucket, path, file_name, creds=self.auth)
            if self.del_file:
                os.remove(file_name)
