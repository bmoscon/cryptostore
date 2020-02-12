'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
class Store:
    def write(self, exchange: str, data_type: str, pair: str, timestamp: float):
        raise NotImplementedError

    def aggregate(self, data: dict, transform=lambda x: x):
        raise NotImplementedError

    def get_start_date(self, exchange: str, data_type:str, pair: str) -> float:
        raise NotImplementedError
