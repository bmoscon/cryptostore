'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import asyncio
from multiprocessing import Process
import time
import logging
import os

from cryptostore.aggregator.redis import Redis
from cryptostore.aggregator.kafka import Kafka
from cryptostore.data.storage import Storage
from cryptostore.config import DynamicConfig


LOG = logging.getLogger('cryptostore')


class Aggregator(Process):
    def __init__(self, config_file=None):
        self.config_file = config_file
        super().__init__()
        self.daemon = True

    def run(self):
        LOG.info("Aggregator running on PID %d", os.getpid())
        loop = asyncio.get_event_loop()
        self.config = DynamicConfig(file_name=self.config_file)
        loop.create_task(self.loop())
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass

    async def loop(self):
        if self.config.cache == 'redis':
            cache = Redis(self.config.redis['ip'],
                          self.config.redis['port'],
                          del_after_read=self.config.redis['del_after_read'],
                          flush=self.config.redis['start_flush'])
        elif self.config.cache == 'kafka':
            cache = Kafka(self.config.kafka['ip'],
                          self.config.kafka['port'],
                          flush=self.config.kafka['start_flush'])

        while True:
            start = time.time()
            if 'exchanges' in self.config and self.config.exchanges:
                for exchange in self.config.exchanges:
                    for dtype in self.config.exchanges[exchange]:
                        for pair in self.config.exchanges[exchange][dtype]:
                            store = Storage(self.config)
                            LOG.info('Reading %s-%s-%s', exchange, dtype, pair)

                            data = cache.read(exchange, dtype, pair)
                            if len(data) == 0:
                                LOG.info('No data for %s-%s-%s', exchange, dtype, pair)
                                continue

                            store.aggregate(data)
                            store.write(exchange, dtype, pair, time.time())

                            cache.delete(exchange, dtype, pair)
                            LOG.info('Write Complete %s-%s-%s', exchange, dtype, pair)

                total = time.time() - start
                interval = self.config.storage_interval - total
                if interval <= 0:
                    LOG.warning("Storage operations currently take %.1f seconds, longer than the interval of %d", total, self.config.storage_interval)
                    interval = 0.5
                await asyncio.sleep(interval)
            else:
                await asyncio.sleep(30)
