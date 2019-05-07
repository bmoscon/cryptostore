'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import asyncio
import os

import yaml


class Config:
    def __init__(self, file_name=None, reload_interval=10, callback=None):
        if file_name is None:
            if 'CRYPTOSTORE_CONFIG' in os.environ:
                file_name = os.environ['CRYPTOSTORE_CONFIG']
            else:
                file_name = os.path.join(os.getcwd(), 'config.yaml')
        if not os.path.isfile(file_name):
            raise FileNotFoundError(f"Config file {file_name} not found")

        self.config = {}
        self._load(file_name, reload_interval, callback)

    def __getattr__(self, attr):
        return self.config[attr]

    def __contains__(self, item):
        return item in self.config

    async def __loader(self, file, interval, callback):
        last_modified = 0
        while True:
            cur_mtime = os.stat(file).st_mtime
            if cur_mtime != last_modified:
                with open(file, 'r') as fp:
                    self.config = yaml.load(fp)
                    if callback is not None:
                        await callback(self.config)
                    last_modified = cur_mtime

            await asyncio.sleep(interval)

    def _load(self, file, interval, callback):
        loop = asyncio.get_event_loop()
        loop.create_task(self.__loader(file, interval, callback))
