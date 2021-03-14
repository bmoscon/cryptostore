'''
Copyright (C) 2018-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''


class StorageEngine:
    def __init__(self):
        self._engines = {}

    def __getitem__(self, engine):
        if engine not in self._engines:
            self._engines[engine] = __import__(engine)
        return self._engines[engine]

    def __getattr__(self, engine):
        if engine not in self._engines:
            self._engines[engine] = __import__(engine)
        return self._engines[engine]


StorageEngines = StorageEngine()
