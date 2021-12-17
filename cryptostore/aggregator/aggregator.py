'''
Copyright (C) 2018-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import asyncio
from multiprocessing import Process
import time
import logging
import os
from datetime import timedelta

from cryptostore.util import get_time_interval, setup_event_loop_signal_handlers, stop_event_loop
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
        self.terminating = False

    def run(self):
        LOG.info("Aggregator running on PID %d", os.getpid())
        loop = asyncio.get_event_loop()
        self.config = DynamicConfig(loop=loop, file_name=self.config_file)
        loop.create_task(self.loop())
        setup_event_loop_signal_handlers(loop, self._stop_on_signal)
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass
        except Exception:
            LOG.error("Aggregator running on PID %d died due to exception", os.getpid(), exc_info=True)

    def _stop_on_signal(self, sig, loop):
        if self.terminating:
            LOG.info("Aggregator on PID %d is already being stopped...", os.getpid())
            return

        LOG.info("Stopping Aggregator on %d due to signal %d", os.getpid(), sig)
        self.terminating = True
        self.config.set_terminating()
        if 'write_on_stop' in self.config and self.config.write_on_stop \
                and 'exchanges' in self.config and self.config.exchanges:
            stop_event_loop(loop, self._write_storage(write_on_stop=True))
        else:
            stop_event_loop(loop)

    async def loop(self):
        if self.config.cache == 'redis':
            self.cache = Redis(ip=self.config.redis['ip'],
                          port=self.config.redis['port'],
                          password=os.environ.get('REDIS_PASSWORD', None) or self.config.redis.get('password', None),
                          socket=self.config.redis.socket,
                          del_after_read=self.config.redis['del_after_read'],
                          flush=self.config.redis['start_flush'],
                          retention=self.config.redis.retention_time if 'retention_time' in self.config.redis else None)
        elif self.config.cache == 'kafka':
            self.cache = Kafka(self.config.kafka['ip'],
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

        self.parquet_buffer = dict()
        while not self.terminating:
            start, end = None, None
            try:
                aggregation_start = time.time()
                if time_partition:
                    interval_start = aggregation_start
                    if end:
                        interval_start = end + timedelta(seconds=interval + 1)
                    start, end = get_time_interval(interval_start, base_interval, multiplier=multiplier)
                if 'exchanges' in self.config and self.config.exchanges:
                    await self._write_storage(start=start, end=end)
                    total = time.time() - aggregation_start
                    wait = interval - total
                    if wait <= 0:
                        LOG.warning("Storage operations currently take %.1f seconds, longer than the interval of %d", total, interval)
                        wait = 0.5
                    try:
                        await asyncio.sleep(wait)
                    except asyncio.CancelledError as e:
                        pass
                else:
                    try:
                        await asyncio.sleep(30)
                    except asyncio.CancelledError as e:
                        pass
            except Exception:
                LOG.error("Aggregator running on PID %d died due to exception", os.getpid(), exc_info=True)
                raise

        LOG.info("Aggregator running on PID %d stopped", os.getpid())

    async def _write_storage(self, start=None, end=None, write_on_stop=False):
        if write_on_stop:
            LOG.info("Writing cached data before stopping...")
        store = Storage(self.config, parquet_buffer=self.parquet_buffer)
        for exchange in self.config.exchanges:
            for dtype in self.config.exchanges[exchange]:
                # Skip over the retries arg in the config if present.
                if dtype in {'retries', 'channel_timeouts', 'http_proxy'}:
                    continue
                for pair in self.config.exchanges[exchange][dtype] if 'symbols' not in \
                                                                      self.config.exchanges[exchange][dtype] else \
                self.config.exchanges[exchange][dtype]['symbols']:
                    LOG.info('Reading cache for %s-%s-%s', exchange, dtype, pair)
                    data = self.cache.read(exchange, dtype, pair, start=start, end=end)
                    if len(data) == 0:
                        LOG.info('No data for %s-%s-%s', exchange, dtype, pair)
                        continue

                    store.aggregate(data)

                    retries = 0
                    while ((not self.terminating) or write_on_stop):
                        if retries > self.config.storage_retries:
                            LOG.error("Failed to write after %d retries", self.config.storage_retries)
                            raise EngineWriteError

                        try:
                            # retrying this is ok, provided every
                            # engine clears its internal buffer after writing successfully.
                            LOG.info('Writing cached data to store for %s-%s-%s', exchange, dtype, pair)
                            store.write(exchange, dtype, pair, time.time())
                        except OSError as e:
                            LOG.warning('Could not write %s-%s-%s. %s', exchange, dtype, pair, e)
                            if write_on_stop:
                                break
                            if e.errno == 112:  # Host is down
                                retries += 1
                                try:
                                    await asyncio.sleep(self.config.storage_retry_wait)
                                except asyncio.CancelledError as e:
                                    pass
                                continue
                            else:
                                raise

                        except EngineWriteError:
                            LOG.warning('Could not write %s-%s-%s. %s', exchange, dtype, pair, e)
                            if write_on_stop:
                                break
                            retries += 1
                            try:
                                await asyncio.sleep(self.config.storage_retry_wait)
                            except asyncio.CancelledError as e:
                                pass
                            continue
                        else:
                            break

                    LOG.info('Deleting cached data for %s-%s-%s', exchange, dtype, pair)
                    self.cache.delete(exchange, dtype, pair)
                    LOG.info('Write Complete %s-%s-%s', exchange, dtype, pair)