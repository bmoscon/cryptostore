'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import os
import glob

import pyarrow as pa
import pyarrow.parquet as pq

from cryptostore.data.store import Store
from cryptostore.data.gc import google_cloud_write, google_cloud_read, google_cloud_list
from cryptostore.data.s3 import aws_write, aws_read, aws_list
from cryptostore.data.gd import google_drive_write, google_drive_read, google_drive_list
from cryptostore.exceptions import InconsistentStorage


class Parquet(Store):
    def __init__(self, config=None):
        self._write = []
        self._read = []
        self._list = []
        self.bucket = []
        self.kwargs = []
        self.prefix = []
        self.data = None
        self.del_file = True
        self.file_name = config.get('file_format') if config else None
        self.path = config.get('path') if config else None

        if config:
            self.del_file = config.get('del_file', True)

            if 'GCS' in config:
                self._write.append(google_cloud_write)
                self._read.append(google_cloud_read)
                self._list.append(google_cloud_list)
                self.bucket.append(config['GCS']['bucket'])
                self.prefix.append(config['GCS']['prefix'])
                self.kwargs.append({'creds': config['GCS']['service_account']})
            if 'S3' in config:
                self._write.append(aws_write)
                self._read.append(aws_read)
                self._list.append(aws_list)
                self.bucket.append(config['S3']['bucket'])
                self.prefix.append(config['S3']['prefix'])
                self.kwargs.append({'creds': (config['S3']['key_id'], config['S3']['secret']), 'endpoint': config['S3'].get('endpoint')})
            if 'GD' in config:
                self._write.append(google_drive_write)
                self._read.append(google_drive_read)
                self._list.append(google_drive_list)
                self.prefix.append(config['GD']['prefix'])
                self.bucket.append(None)
                self.kwargs.append({'creds': config['GD']['service_account']})


    def aggregate(self, data):
        names = list(data[0].keys())
        cols = {name : [] for name in names}

        for entry in data:
            for key in entry:
                val = entry[key]
                cols[key].append(val)
        arrays = [pa.array(cols[col]) for col in cols]
        table = pa.Table.from_arrays(arrays, names=names)
        self.data = table

    def write(self, exchange, data_type, pair, timestamp):
        if not self.data:
            return
        file_name = ''
        if self.file_name:
            for var in self.file_name:
                if var == 'timestamp':
                    file_name += f"{int(timestamp)}-"
                elif var == 'data_type':
                    file_name += f"{data_type}-"
                elif var == "exchange":
                    file_name += f"{exchange}-"
                elif var == "pair":
                    file_name += f"{pair}-"
                else:
                    print(var)
                    raise ValueError("Invalid file format specified for parquet file")
            file_name = file_name[:-1] + ".parquet"
        else:
            file_name = f'{exchange}-{data_type}-{pair}-{int(timestamp)}.parquet'

        if self.path:
            file_name = os.path.join(self.path, file_name)

        pq.write_table(self.data, file_name)
        self.data = None

        if self._write:
            print(self._write)
            print(self.bucket)
            print(self.prefix)
            print(self.kwargs)
            for func, bucket, prefix, kwargs in zip(self._write, self.bucket, self.prefix, self.kwargs):
                path = f'{exchange}/{data_type}/{pair}/{exchange}-{data_type}-{pair}-{int(timestamp)}.parquet'
                if prefix:
                    path = f"{prefix}/{path}"
                func(bucket, path, file_name, **kwargs)
            if self.del_file:
                os.remove(file_name)

    def get_start_date(self, exchange: str, data_type: str, pair: str) -> float:
        objs = []
        files = []

        if not self.del_file:
            file_pattern = f'{exchange}-{data_type}-{pair}-[0-9]*.parquet'
            files = glob.glob(file_pattern)

        if self._read:
            for func, bucket, prefix, kwargs in zip(self._list, self.bucket, self.prefix, self.kwargs):
                path = f'{exchange}/{data_type}/{pair}/'
                if prefix:
                    path = f"{prefix}/{path}"
                ret = func(bucket, path, limit=1, **kwargs)
                objs.append(ret)
        if not files and not any(objs):
            return None

        if files:
            files = sorted(files)
            start = files[0]
        else:
            if isinstance(objs[0][0], tuple):
                # Google Drive list() returns a list of tuples.
                start = objs[0][0][0]
            else:
                start = objs[0][0]

        for entry in objs:
            if entry[0] != start:
                raise InconsistentStorage("Stored data differs, cannot backfill")

        if files:
            return float(pq.read_table(files[0], columns=['timestamp']).to_pandas().timestamp[0])
        else:
            tmp = f'{exchange}-{pair}-temp.parquet'
            self._read[0](self.bucket[0], objs[0][0], tmp, **self.kwargs[0])
            start = float(pq.read_table(tmp, columns=['timestamp']).to_pandas().timestamp[0])
            os.remove(tmp)
            return start
