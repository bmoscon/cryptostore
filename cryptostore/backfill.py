'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import time
import logging
from multiprocessing import Process
from pandas import Timestamp

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
            if end <= Timestamp(start):
                LOG.info("Data in storage is earlier than backfill start date for %s - %s", self.exchange, pair)
                continue

            LOG.info("Backfill - Starting for %s - %s for range %s - %s", self.exchange, pair, start, str(end))

            for trades in r[self.exchange].trades(pair, start, str(end)):
                if not trades:
                    continue
                period_start = Timestamp(trades[0]['timestamp'], unit='s')
                period_end = Timestamp(trades[-1]['timestamp'], unit='s')
                if period_start >= Timestamp(end, unit='s'):
                    LOG.warning("Backfill - Period start exceeds end timestamp")
                    # should never happen
                    break
                storage.aggregate(trades)
                storage.write(self.exchange, 'trades', pair, period_end.timestamp())
                LOG.info("Backfill - Wrote %s to %s for %s - %s", period_start, period_end, self.exchange, pair)
            LOG.info("Backfill for %s - %s completed", self.exchange, pair)
