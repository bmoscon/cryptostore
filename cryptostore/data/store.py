'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
class Store:
    def write(self, exchange, data_type, pair, timestamp):
        raise NotImplementedError

    def aggregate(self, data):
        raise NotImplementedError
