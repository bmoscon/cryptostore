'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

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
        except Exception:
            LOG.error("Aggregator running on PID %d died due to exception", os.getpid(), exc_info=True)

    async def loop(self):
        if self.config.cache == 'redis':
            cache = Redis(ip=self.config.redis['ip'],
                          port=self.config.redis['port'],
                          socket=self.config.redis.socket,
                          del_after_read=self.config.redis['del_after_read'],
                          flush=self.config.redis['start_flush'],
                          retention=self.config.redis.retention_time if 'retention_time' in self.config.redis else None)
        elif self.config.cache == 'kafka':
            cache = Kafka(self.config.kafka['ip'],
                          self.config.kafka['port'],
                          flush=self.config.kafka['start_flush'])

        while True:
            try:
                start = time.time()
                if 'exchanges' in self.config and self.config.exchanges:
                    for exchange in self.config.exchanges:
                        for dtype in self.config.exchanges[exchange]:
                            if dtype in {'retries'}:
                                continue
                            for pair in self.config.exchanges[exchange][dtype] if 'symbols' not in self.config.exchanges[exchange][dtype] else self.config.exchanges[exchange][dtype]['symbols']:
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
            except Exception:
                LOG.error("Aggregator running on PID %d died due to exception", os.getpid(), exc_info=True)
                raise
