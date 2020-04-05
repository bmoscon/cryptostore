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


LOG = logging.getLogger('cryptostore')


class MissingTrades(Plugin):
    def __init__(self, config):
        super().__init__(config)
        self.config = Config(config)
        self.daemon = True
        self.threads = []

    def _worker(self, exchange):
        r = Rest()
        storage = Storage(self.config)
        for pair in self.config.missing_trades[exchange]:
            try:
                start = self.config.missing_trades[exchange][pair].start
                lu = self.config.missing_trades
                if lu and exchange in lu and pair in lu[exchange]:
                    start = lu[exchange][pair]

                LOG.info("Backfill - Starting for %s - %s for range %s - %s", exchange, pair, start, str(max(ends)))

                start = Timestamp(start)
                while start < Timestamp.now():
                    end = (start + Timedelta(1, unit='d')).replace(hour=0, minute=0, second=0, microsecond=0, nanosecond=0) - Timedelta(1, unit='ns')
                    LOG.info("Backfill - Reading %s to %s for %s - %s", start, end, exchange, pair)

                    trades = []
                    try:
                        for t in r[exchange].trades(pair, str(start), str(end)):
                            trades.extend(t)
                    except Exception:
                        LOG.warning("Backfill - encountered error backfilling %s - %s, trying again...", exchange, pair, exc_info=True)
                        time.sleep(300)
                        continue

                    if not trades:
                        start = end + Timedelta(1, unit='ns')
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
