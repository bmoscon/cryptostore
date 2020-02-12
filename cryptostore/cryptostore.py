'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from multiprocessing import Queue
import asyncio
import logging
import json
import os

from cryptostore.spawn import Spawn
from cryptostore.config import DynamicConfig
from cryptostore.log import get_logger
from cryptostore.aggregator.aggregator import Aggregator
from cryptostore.plugin.controller import PluginController


LOG = get_logger('cryptostore', 'cryptostore.log', logging.INFO, size=50000000, num_files=10)


class Cryptostore:
    def __init__(self, config=None):
        self.queue = Queue()
        self.spawner = Spawn(self.queue)
        self.running_config = {}
        self.cfg_path = config
        self.plugin = PluginController(config)
        self.plugin.start()

    async def _load_config(self, start, stop):
        LOG.info("start: %s stop: %s", str(start), str(stop))
        for exchange in stop:
            self.queue.put(json.dumps({'op': 'stop', 'exchange': exchange}))

        for exchange in start:
            self.queue.put(json.dumps({'op': 'start', 'exchange': exchange, 'collector': self.running_config['exchanges'][exchange], 'config': {i : self.running_config[i] for i in self.running_config if i != 'exchanges'}}))

    async def _reconfigure(self, config):
        stop = []
        start = []

        if self.running_config != config:
            if not config or 'exchanges' not in config or not config['exchanges'] or len(config['exchanges']) == 0:
                # shut it all down
                stop = list(self.running_config['exchanges'].keys()) if 'exchanges' in self.running_config else []
                self.running_config = config
            elif not self.running_config or 'exchanges' not in self.running_config or len(self.running_config['exchanges']) == 0:
                # nothing running currently, start it all
                self.running_config = config
                start = list(self.running_config['exchanges'].keys())
            else:
                for e in config['exchanges']:
                    if e in self.running_config['exchanges'] and config['exchanges'][e] == self.running_config['exchanges'][e]:
                        continue
                    elif e not in self.running_config['exchanges']:
                        start.append(e)
                    else:
                        stop.append(e)
                        start.append(e)

                for e in self.running_config['exchanges']:
                    if e in config['exchanges'] and config['exchanges'][e] == self.running_config['exchanges'][e]:
                        continue
                    elif e not in config['exchanges']:
                        stop.append(e)
                    else:
                        stop.append(e)
                        start.append(e)
            self.running_config = config
        await self._load_config(list(set(start)), list(set(stop)))

    def run(self):
        LOG.info("Starting cryptostore")
        LOG.info("Cryptostore running on PID %d", os.getpid())

        self.spawner.start()
        LOG.info("Spawner started")

        self.aggregator = Aggregator(config_file=self.cfg_path)
        self.aggregator.start()
        LOG.info("Aggregator started")

        loop = asyncio.get_event_loop()
        self.config = DynamicConfig(file_name=self.cfg_path, callback=self._reconfigure)

        LOG.info("Cryptostore started")
        loop.run_forever()
