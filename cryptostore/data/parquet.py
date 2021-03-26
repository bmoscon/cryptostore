'''
Copyright (C) 2018-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import os
import glob
import datetime

import pyarrow as pa
import pyarrow.parquet as pq

from cryptostore.data.store import Store
from cryptostore.data.gc import google_cloud_write, google_cloud_read, google_cloud_list
from cryptostore.data.s3 import aws_write, aws_read, aws_list
from cryptostore.data.gd import GDriveConnector
from cryptostore.exceptions import InconsistentStorage


class Parquet(Store):

    def __init__(self, exchanges, config=None, parquet_buffer=None):
        self._write = []
        self._read = []
        self._list = []
        self.bucket = []
        self.kwargs = []
        self.prefix = []
        self.data = None
        self.del_file = True
        self.file_name = None
        self.path = None
        self.prefix_date = False
        self.comp_codec = None
        self.comp_level = None
        self.buffer = parquet_buffer
        if config:
            self.file_name = config.get('file_format')
            self.path = config.get('path') or os.getcwd()
            self.prefix_date = config.get('prefix_date', False)
            self.del_file = config.get('del_file', True)
            # Compression
            if 'compression' in config:
                self.comp_codec = config['compression']['codec']
                if 'level' in config['compression']:
                    self.comp_level = config['compression']['level']
            # Connectors
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
                folder_name_sep = config['GD']['folder_name_sep'] if 'folder_name_sep' in config['GD'] else '-'
                g_drive = GDriveConnector(config['GD']['service_account'], exchanges, config['GD']['prefix'], folder_name_sep, self._default_path)
                self._write.append(g_drive.write)
                self.prefix.append(None)
                self.bucket.append(None)
                self.kwargs.append({'dummy': 'Dummy'})
            # Counter
            self.append_counter = config.get('append_counter') if 'append_counter' in config else 0

    def _default_path(self, exchange, data_type, pair):
        return f'{exchange}/{data_type}/{pair}'

    def aggregate(self, data):
        if isinstance(data[0], dict):
            # Case `data` is a list or tuple of dict.
            names = list(data[0])
        else:
            # Case `data` is a tuple with tuple of keys of dict as 1st parameter,
            # and generator of dicts as 2nd paramter.
            names = data[0]
            data = data[1]

        cols = {name: [] for name in names}
        for entry in data:
            for key in entry:
                val = entry[key]
                cols[key].append(val)

        to_dict = ('feed', 'symbol', 'side')
        to_double = ('size', 'amount')
        arrays = [pa.array(cols[col], pa.string()).dictionary_encode() if col in to_dict
                  else pa.array(cols[col], pa.float64()) if col in to_double
                  else pa.array(cols[col]) for col in cols]
        table = pa.Table.from_arrays(arrays, names=names)
        self.data = table

    def write(self, exchange, data_type, pair, timestamp):
        if not self.data:
            return
        file_name = ''
        timestamp = str(int(timestamp))
        if self.file_name:
            for var in self.file_name:
                if var == 'timestamp':
                    file_name += f"{timestamp}-"
                elif var == 'data_type':
                    file_name += f"{data_type}-"
                elif var == "exchange":
                    file_name += f"{exchange}-"
                elif var == "symbol":
                    file_name += f"{pair}-"
                else:
                    raise ValueError("Invalid file format specified for parquet file")
            file_name = file_name[:-1] + ".parquet"
        else:
            file_name = f'{exchange}-{data_type}-{pair}-{timestamp}.parquet'

        f_name_tips = tuple(file_name.split(timestamp))

        if self.path and self.prefix_date:
            date = str(datetime.date.fromtimestamp(int(timestamp)))
            local_path = os.path.join(self.path, date)
        else:
            local_path = self.path
        save_path = os.path.join(self.path, "temp") if self.append_counter else local_path

        # Write parquet file and manage `counter`.
        if f_name_tips not in self.buffer:
            # Case 'create new parquet file'.
            file_name += '.tmp' if self.append_counter else ''

            if self.path:
                os.makedirs(save_path, mode=0o755, exist_ok=True)
                save_path = os.path.join(save_path, file_name)

            writer = pq.ParquetWriter(save_path, self.data.schema, compression=self.comp_codec, compression_level=self.comp_level)
            writer.write_table(table=self.data)
            self.buffer[f_name_tips] = {'counter': 0, 'writer': writer, 'timestamp': timestamp}
        else:
            # Case 'append existing parquet file'.
            writer = self.buffer[f_name_tips]['writer']
            writer.write_table(table=self.data)
            self.buffer[f_name_tips]['counter'] += 1

        self.data = None

        # If `append_counter` is reached, close parquet file and reset `counter`.
        if self.buffer[f_name_tips]['counter'] == self.append_counter:
            writer.close()
            if self._write or self.append_counter:
                timestamp = self.buffer[f_name_tips]['timestamp']
                file_name = f_name_tips[0] + timestamp + f_name_tips[1]
                if self.path:
                    os.makedirs(local_path, mode=0o755, exist_ok=True)
                    final_path = os.path.join(local_path, file_name)
                if self.append_counter:
                    # Remove '.tmp' suffix
                    os.rename(os.path.join(save_path, file_name + ".tmp"), final_path)
                if self._write:
                    # Upload in cloud storage (GCS, S3 or GD)
                    for func, bucket, prefix, kwargs in zip(self._write, self.bucket, self.prefix, self.kwargs):
                        path = self._default_path(exchange, data_type, pair) + f'/{exchange}-{data_type}-{pair}-{timestamp}.parquet'
                        if prefix:
                            path = f"{prefix}/{path}"
                        elif self.prefix_date:
                            date = str(datetime.date.fromtimestamp(int(timestamp)))
                            path = f"{date}/{path}"
                        func(bucket, path, final_path, **kwargs)
                    if self.del_file:
                        os.remove(final_path)
            # Reset counter
            del self.buffer[f_name_tips]

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
