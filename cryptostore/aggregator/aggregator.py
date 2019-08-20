"""
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
"""
import asyncio
from multiprocessing import Process
import time
import logging
import os

from cryptostore.aggregator.redis import Redis
from cryptostore.aggregator.kafka import Kafka
from cryptostore.data.storage import Storage
from cryptostore.config import DynamicConfig


LOG = logging.getLogger("cryptostore")


class Aggregator(Process):
    """
    This class makes the connection between redis/kafka and the persistent 
    storage medium/s of choice.
    
    It does so by reading the data currently cached in memory and writing it to
    the persistent medium. This is necessary because of the many messages that a
    data feed can produce, as it would be inefficient to be writing each
    message to a database as it is received by cryptostore.
    
    The default interval time is 60 seconds and it is defined in the
    config_file. 
    """

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
        if self.config.cache == "redis":
            cache = Redis(
                self.config.redis["ip"],
                self.config.redis["port"],
                del_after_read=self.config.redis["del_after_read"],
                flush=self.config.redis["start_flush"],
                wide_tables=self.config.wide_tables,
            )
        elif self.config.cache == "kafka":
            cache = Kafka(
                self.config.kafka["ip"],
                self.config.kafka["port"],
                flush=self.config.kafka["start_flush"],
                wide_tables=self.config.wide_tables,
            )

        while True:
            start = time.time()
            if "exchanges" in self.config and self.config.exchanges:
                for exchange in self.config.exchanges:
                    for dtype in self.config.exchanges[exchange]:
                        if dtype in {"retries"}:
                            continue
                        for pair in (
                            self.config.exchanges[exchange][dtype]
                            if "symbols" not in self.config.exchanges[exchange][dtype]
                            else self.config.exchanges[exchange][dtype]["symbols"]
                        ):
                            # All the storage mediums defined in the config_file
                            store = Storage(self.config)
                            LOG.info("Reading %s-%s-%s", exchange, dtype, pair)

                            # Read from either redis or kafka
                            data = cache.read(exchange, dtype, pair)
                            if len(data) == 0:
                                LOG.info("No data for %s-%s-%s", exchange, dtype, pair)
                                continue

                            # Aggregate the data to write on each defined
                            # storage medium - This is so, because different
                            # storage mediums/databases may write data in a
                            # different format.
                            store.aggregate(data)

                            # Write the data on each of the storage mediums
                            # defined.
                            store.write(exchange, dtype, pair, time.time())

                            # Delete the cached data as it is no longer
                            # necessary.
                            cache.delete(exchange, dtype, pair)
                            LOG.info("Write Complete %s-%s-%s", exchange, dtype, pair)

                total = time.time() - start
                interval = self.config.storage_interval - total

                # This is issued as a warning if you are taking more time to
                # store data, than the interval you have defined to cache it.
                # In other words, you can't keep up with the amount of data you
                # are receiving.
                if interval <= 0:
                    LOG.warning(
                        "Storage operations currently take %.1f seconds, longer than the interval of %d",
                        total,
                        self.config.storage_interval,
                    )
                    interval = 0.5
                await asyncio.sleep(interval)
            else:
                await asyncio.sleep(30)
