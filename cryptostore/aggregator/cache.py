"""
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
"""


class Cache:
    """
    Interface for the redis and kafka subclasses. 
    Defines which methods should be implemented.
    """

    def read(self, exchange, dtype, pair):
        raise NotImplementedError

    def delete(self, exchange, dtype, pair):
        raise NotImplementedError
