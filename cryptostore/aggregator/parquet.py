'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import pyarrow as pa
import pyarrow.parquet as pq

from cryptostore.aggregator.store import Store


class Parquet(Store):
    def __init__(self):
        self.data = None
    
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

    def write(self, file_name):
        pq.write_table(self.data, file_name)
        self.data = None
