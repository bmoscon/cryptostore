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
from datetime import timedelta

from cryptostore.util import get_time_interval
from cryptostore.aggregator.redis import Redis
from cryptostore.aggregator.kafka import Kafka
from cryptostore.data.storage import Storage
from cryptostore.config import DynamicConfig
from cryptostore.exceptions import EngineWriteError


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

        interval = self.config.storage_interval
        time_partition = False
        multiplier = 1
        if not isinstance(interval, int):
            if len(interval) > 1:
                multiplier = int(interval[:-1])
                interval = interval[-1]
            base_interval = interval
            if interval in {'M', 'H', 'D'}:
                time_partition = True
                if interval == 'M':
                    interval = 60 * multiplier
                elif interval == 'H':
                    interval = 3600 * multiplier
                else:
                    interval = 86400 * multiplier

        parquet_buffer = dict()
        while True:
            start, end = None, None
            try:
                aggregation_start = time.time()
                if time_partition:
                    interval_start = aggregation_start
                    if end:
                        interval_start = end + timedelta(seconds=interval + 1)
                    start, end = get_time_interval(interval_start, base_interval, multiplier=multiplier)
                if 'exchanges' in self.config and self.config.exchanges:
                    store = Storage(self.config, parquet_buffer=parquet_buffer)
                    for exchange in self.config.exchanges:
                        for dtype in self.config.exchanges[exchange]:
                            # Skip over the retries arg in the config if present.
                            if dtype in {'retries', 'channel_timeouts'}:
                                continue
                            for pair in self.config.exchanges[exchange][dtype] if 'symbols' not in self.config.exchanges[exchange][dtype] else self.config.exchanges[exchange][dtype]['symbols']:
                                LOG.info('Reading %s-%s-%s', exchange, dtype, pair)
                                data = cache.read(exchange, dtype, pair, start=start, end=end)
                                if len(data) == 0:
                                    LOG.info('No data for %s-%s-%s', exchange, dtype, pair)
                                    continue

                                store.aggregate(data)

                                retries = 0
                                while True:
                                    if retries > self.config.storage_retries:
                                        LOG.error("Failed to write after %d retries", self.config.storage_retries)
                                        raise EngineWriteError

                                    try:
                                        # retrying this is ok, provided every
                                        # engine clears its internal buffer after writing successfully.
                                        store.write(exchange, dtype, pair, time.time())
                                    except OSError as e:
                                        if e.errno == 112: # Host is down
                                            LOG.warning('Could not write %s-%s-%s. %s', exchange, dtype, pair, e)
                                            retries += 1
                                            await asyncio.sleep(self.config.storage_retry_wait)
                                            continue
                                        else:
                                            raise

                                    except EngineWriteError:
                                        retries += 1
                                        await asyncio.sleep(self.config.storage_retry_wait)
                                        continue
                                    else:
                                        break

                                cache.delete(exchange, dtype, pair)
                                LOG.info('Write Complete %s-%s-%s', exchange, dtype, pair)
                    total = time.time() - aggregation_start
                    wait = interval - total
                    if wait <= 0:
                        LOG.warning("Storage operations currently take %.1f seconds, longer than the interval of %d", total, interval)
                        wait = 0.5
                    await asyncio.sleep(wait)
                else:
                    await asyncio.sleep(30)
            except Exception:
                LOG.error("Aggregator running on PID %d died due to exception", os.getpid(), exc_info=True)
                raise
