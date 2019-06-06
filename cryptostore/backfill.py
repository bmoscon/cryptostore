'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import time
import logging
from multiprocessing import Process
from pandas import Timestamp, Timedelta

from cryptofeed.rest import Rest

from cryptostore.data.storage import Storage
from cryptostore.exceptions import InconsistentStorage


LOG = logging.getLogger('cryptostore')


class Backfill(Process):
    def __init__(self, exchange, config):
        self.exchange = exchange
        self.config = config
        super().__init__()

    def run(self):
        r = Rest()
        storage = Storage(self.config)
        for pair in self.config.backfill[self.exchange]:
            try:
                start = self.config.backfill[self.exchange][pair]['start']

                while True:
                    end = storage.get_start_date(self.exchange, 'trades', pair)
                    if not all(e == end[0] for e in end):
                        raise InconsistentStorage("Stored data differs, cannot backfill")
                    end = end[0]
                    if end:
                        break
                    time.sleep(10)
                end = Timestamp(end, unit='s')
                end -= Timedelta(microseconds=1)
                start = Timestamp(start)
                if end <= Timestamp(start):
                    LOG.info("Data in storage is earlier than backfill start date for %s - %s", self.exchange, pair)
                    continue

                LOG.info("Backfill - Starting for %s - %s for range %s - %s", self.exchange, pair, start, str(end))

                # Backfill from end date to start date, 1 day at a time, in reverse order (from end -> start)
                while start < end:
                    seg_start = end.replace(hour=0, minute=0, second=0, microsecond=0, nanosecond=0)
                    LOG.info("Backfill - Reading %s to %s for %s - %s", seg_start, end, self.exchange, pair)

                    trades = []
                    for t in r[self.exchange].trades(pair, str(seg_start), str(end)):
                        trades.extend(t)
                    if not trades:
                        end = seg_start - Timedelta(nanoseconds=1)
                        continue

                    storage.aggregate(trades)
                    storage.write(self.exchange, 'trades', pair, end.timestamp())
                    LOG.info("Backfill - Wrote %s to %s for %s - %s", seg_start, end, self.exchange, pair)
                    end = seg_start - Timedelta(nanoseconds=1)
                LOG.info("Backfill for %s - %s completed", self.exchange, pair)
            except Exception:
                LOG.error("Backfill failed for %s - %s", self.exchange, pair, exc_info=True)
