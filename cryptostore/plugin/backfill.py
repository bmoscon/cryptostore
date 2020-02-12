'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import time
import logging
from pandas import Timestamp, Timedelta
from threading import Thread
import os
import copy

from cryptofeed.rest import Rest

from cryptostore.config import Config
from cryptostore.plugin.plugin import Plugin
from cryptostore.data.storage import Storage
from cryptostore.exceptions import InconsistentStorage


LOG = logging.getLogger('cryptostore')


class Backfill(Plugin):
    def __init__(self, config):
        super().__init__(config)
        self.config = Config(config)
        self.daemon = True
        self.threads = []

    def _worker(self, exchange):
        r = Rest()
        storage = Storage(self.config)
        for pair in self.config.backfill[exchange]:
            try:
                start = self.config.backfill[exchange][pair].start

                while True:
                    end = storage.get_start_date(exchange, 'trades', pair)

                    if all(e for e in end):
                        break
                    time.sleep(10)
                ends = list(map(lambda x: Timestamp(x, unit='s') - Timedelta(microseconds=1), end))

                if any(e <= Timestamp(start) for e in ends):
                    LOG.info("Data in storage is earlier than backfill start date for %s - %s", exchange, pair)
                    continue

                LOG.info("Backfill - Starting for %s - %s for range %s - %s", exchange, pair, start, str(max(ends)))

                # Backfill from end date to start date, 1 day at a time, in reverse order (from end -> start)
                end = max(ends)
                start = Timestamp(start)
                while start < end:
                    seg_start = end.replace(hour=0, minute=0, second=0, microsecond=0, nanosecond=0)
                    if start > seg_start:
                        seg_start = start
                    LOG.info("Backfill - Reading %s to %s for %s - %s", seg_start, end, exchange, pair)

                    trades = []
                    try:
                        for t in r[exchange].trades(pair, str(seg_start), str(end)):
                            trades.extend(t)
                    except Exception:
                        LOG.warning("Backfill - encountered error backfilling %s - %s, trying again...", exchange, pair, exc_info=True)
                        time.sleep(300)
                        continue

                    if not trades:
                        end = seg_start - Timedelta(nanoseconds=1)
                        continue

                    for trade in trades:
                        trade['price'] = float(trade['price'])
                        trade['amount'] = float(trade['amount'])

                    def gen_pos():
                        counter = 0
                        while True:
                            yield counter % len(ends)
                            counter += 1

                    pos = gen_pos()
                    ends_float = [x.timestamp() for x in ends]

                    def timestamp_filter(data):
                        boundary = ends_float[next(pos)]
                        return list(filter(lambda x: x['timestamp'] < boundary, copy.copy(data)))

                    storage.aggregate(trades, transform=timestamp_filter)
                    storage.write(exchange, 'trades', pair, end.timestamp())
                    LOG.info("Backfill - Wrote %s to %s for %s - %s", seg_start, end, exchange, pair)
                    end = seg_start - Timedelta(nanoseconds=1)
                LOG.info("Backfill for %s - %s completed", exchange, pair)
            except Exception:
                LOG.error("Backfill failed for %s - %s", exchange, pair, exc_info=True)

    def run(self):
        LOG.info("Backfill running on PID %d", os.getpid())
        if 'backfill' in self.config:
            for exchange in self.config.backfill:
                self.threads.append(Thread(target=self._worker, args=(exchange,)))
                self.threads[-1].start()

            for t in self.threads:
                t.join()
