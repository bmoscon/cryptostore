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
from cryptostore.data.s3 import aws_write

class Parquet(Store):
    def __init__(self, config=None):
        self._write = []
        self.bucket = []
        self.kwargs = []
        self.prefix = []
        self.data = None

        if config:
            self.del_file = False if 'del_file' in config and config['del_file'] == False else True

            if 'GCS' in config:
                self._write.append(google_cloud_write)
                self.bucket.append(config['GCS']['bucket'])
                self.prefix.append(config['GCS']['prefix'])
                self.kwargs.append({'creds': config['GCS']['service_account']})
            if 'S3' in config:
                self._write.append(aws_write)
                self.bucket.append(config['S3']['bucket'])
                self.prefix.append(config['S3']['prefix'])
                self.kwargs.append({'creds': (config['S3']['key_id'], config['S3']['secret'])})

    def aggregate(self, data):
        names = list(data[0].keys())
        cols = {name : [] for name in names}

        for entry in data:
            for key in entry:
                cols[key].append(entry[key])
        arrays = [pa.array(cols[col]) for col in cols]
        table = pa.Table.from_arrays(arrays, names=names)
        self.data = table

    def write(self, exchange, data_type, pair, timestamp):
        file_name = f'{exchange}-{data_type}-{pair}-{int(timestamp)}.parquet'
        pq.write_table(self.data, file_name)
        self.data = None

        if self._write:
            for func, bucket, prefix, kwargs in zip(self._write, self.bucket, self.prefix, self.kwargs):
                path = f'{exchange}/{data_type}/{pair}/{int(timestamp)}.parquet'
                if prefix:
                    path = f"{prefix}/{path}"
                func(bucket, path, file_name, **kwargs)
            if self.del_file:
                os.remove(file_name)
