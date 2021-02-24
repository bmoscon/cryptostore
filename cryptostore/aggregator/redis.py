'''
Copyright (C) 2018-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import logging
from collections import defaultdict
import time

from cryptofeed.defines import TRADES, L2_BOOK, L3_BOOK, TICKER, FUNDING, OPEN_INTEREST, LIQUIDATIONS

from cryptostore.aggregator.util import l2_book_flatten, l3_book_flatten
from cryptostore.aggregator.cache import Cache
from cryptostore.engines import StorageEngines


LOG = logging.getLogger('cryptostore')


class Redis(Cache):
    def __init__(self, ip=None, port=None, socket=None, del_after_read=True, flush=False, retention=None):
        self.del_after_read = del_after_read
        self.retention = retention
        self.last_id = {}
        self.ids = defaultdict(list)
        if ip and port and socket:
            raise ValueError("Cannot specify ip/port and socket for Redis")
        self.conn = StorageEngines.redis.Redis(ip, port, unix_socket_path=socket, decode_responses=True)
        if flush:
            LOG.info('Flushing cache')
            self.conn.flushall()

    def read(self, exchange, dtype, pair, start=None, end=None):
        """
        Read list of JSON dictionaries from Redis buffer, flatten the dictionaries and transform all
        float stored as string into Python float.
        Data is then returned either as a tuple of dictionaries or as a tuple containing for 1st element
        keys of the dictionaries, and for 2nd element a generator yielding said dictionaries.
        This is depending `dtype`.

        Returns:
            updates (tuple[dict] or Tuple[tuple, Generator):
                Tuple of dictionaries when `dtype` is not a L2 or L3 book.
                Tuple of the tuple of keys as 1st element, and as 2nd element a generator yielding
                dictionaries when `dtype` is a L2 or L3 book.

        """

        key = f'{dtype}-{exchange}-{pair}'
        if start and end:
            data = [[key, self.conn.xrange(key, min=start, max=end)]]
        else:
            data = self.conn.xread({key: '0-0' if key not in self.last_id else self.last_id[key]})

        if len(data) == 0 or len(data[0][1]) == 0:
            return []

        LOG.info("%s: Read %d messages from Redis", key, len(data[0][1]))
        self.ids[key], updates = tuple(zip(*data[0][1]))
        self.last_id[key] = self.ids[key][-1]
        if dtype == L2_BOOK:
            updates = l2_book_flatten(updates)
        elif dtype == L3_BOOK:
            updates = l3_book_flatten(updates)
        elif dtype in {TRADES, TICKER, OPEN_INTEREST, LIQUIDATIONS}:
            as_float = ('size', 'amount', 'price', 'timestamp', 'receipt_timestamp', 'bid', 'ask', 'open_interest', 'leaves_qty')
            are_float = filter(as_float.count, updates[0])
            for k in are_float:
                for update in updates:
                    update[k] = float(update[k])
        elif dtype == FUNDING:
            for update in updates:
                for k in update:
                    try:
                        update[k] = float(update[k])
                    except ValueError:
                        # ignore strings
                        pass

        return updates

    def delete(self, exchange, dtype, pair):
        key = f'{dtype}-{exchange}-{pair}'

        if self.del_after_read:
            if self.retention:
                removal_ts = f'{int((time.time() * 1000) - (self.retention * 1000))}-0'
                self.ids[key] = [i[0] for i in self.conn.xrange(key, max=removal_ts)]
        if self.ids[key]:
            self.conn.xdel(key, *self.ids[key])
            LOG.info("%s: Removed %d entries through id %s", key, len(self.ids[key]), self.ids[key][-1])
        else:
            LOG.info("%s: Removed no Redis entries", key)
        self.ids[key] = []
