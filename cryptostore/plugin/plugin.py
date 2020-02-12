'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from multiprocessing import Process


class Plugin(Process):
    def __init__(self, config: str):
        super().__init__()

    def run(self):
        raise NotImplementedError
