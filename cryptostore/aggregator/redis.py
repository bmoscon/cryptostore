"""
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
"""
import logging
from collections import defaultdict
import json

from cryptofeed.defines import TRADES, L2_BOOK, L3_BOOK

from cryptostore.aggregator.util import book_flatten
from cryptostore.aggregator.cache import Cache
from cryptostore.engines import StorageEngines


LOG = logging.getLogger("cryptostore")


class Redis(Cache):
    """
    This class interacts with the aggregator class (in case Redis was chosen as
    as the cached database).

    At any given time, it caches (and holds) the data being received from 
    the (multiple) data feed/s, until such data is read and later deleted by
    the aggregator class.  
    
    """

    def __init__(self, ip, port, del_after_read=True, flush=False, wide_tables=False):
        self.del_after_read = del_after_read
        self.last_id = {}
        self.ids = defaultdict(list)
        self.wide = wide_tables
        self.conn = StorageEngines.redis.Redis(ip, port, decode_responses=True)
        if flush:
            LOG.info("Flushing cache")
            self.conn.flushall()

    def read(self, exchange, dtype, pair):
        key = f"{dtype}-{exchange}-{pair}"

        data = self.conn.xread(
            {key: "0-0" if key not in self.last_id else self.last_id[key]}
        )

        if len(data) == 0:
            return []

        LOG.info("%s: Read %d messages from Redis", key, len(data[0][1]))
        ret = []

        for update_id, update in data[0][1]:
            if dtype in {L2_BOOK, L3_BOOK}:
                update = json.loads(update["data"])

                if dtype == L2_BOOK and self.wide:
                    # We don't need to run the loop to convert strings into
                    # floats as we know that if "self.wide" is True, then it
                    # means that the data will be stored in arctic as a
                    # DataFrame, and so we can vectorize the operation of
                    # transforming strings to floats.
                    # We simply return a list of dictionaries with each
                    # dictionary being a specific timestamp with all bids/asks
                    # (book_depth) we have defined.
                    ret.append(update)

                else:
                    # If we are storing the data in the default format:
                    update = book_flatten(update, update["timestamp"], update["delta"])
                    for u in update:
                        for k in ("size", "amount", "price", "timestamp"):
                            if k in u:
                                u[k] = float(u[k])
                    ret.extend(update)
            if dtype == TRADES:
                for k in ("size", "amount", "price", "timestamp"):
                    if k in update:
                        update[k] = float(update[k])
                ret.append(update)
            self.ids[key].append(update_id)

        self.last_id[key] = self.ids[key][-1]
        return ret

    def delete(self, exchange, dtype, pair):
        key = f"{dtype}-{exchange}-{pair}"

        if self.del_after_read:
            self.conn.xdel(key, *self.ids[key])
        LOG.info("%s: Removed through id %s", key, self.ids[key][-1])
        self.ids[key] = []
