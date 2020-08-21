'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import logging
from collections import defaultdict
import json
import time

from cryptofeed.defines import TRADES, L2_BOOK, L3_BOOK, TICKER, FUNDING, OPEN_INTEREST, TRADES_SWAP, L2_BOOK_SWAP, TICKER_SWAP, TRADES_FUTURES, L2_BOOK_FUTURES, TICKER_FUTURES

from cryptostore.aggregator.util import book_flatten
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
        key = f'{dtype}-{exchange}-{pair}'

        if start and end:
            data = [[key, self.conn.xrange(key, min=start, max=end)]]
        else:
            data = self.conn.xread({key: '0-0' if key not in self.last_id else self.last_id[key]})

        if len(data) == 0 or len(data[0][1]) == 0:
            return []

        LOG.info("%s: Read %d messages from Redis", key, len(data[0][1]))
        ret = []

        for update_id, update in data[0][1]:
            if dtype in {L2_BOOK, L3_BOOK, L2_BOOK_FUTURES, L2_BOOK_SWAP}:
                update = json.loads(update['data'])
                update = book_flatten(update, update['timestamp'], update['receipt_timestamp'], update['delta'])
                for u in update:
                    for k in ('size', 'amount', 'price', 'timestamp', 'receipt_timestamp'):
                        if k in u:
                            u[k] = float(u[k])
                ret.extend(update)
            elif dtype in {TRADES, TRADES_FUTURES, TRADES_SWAP, TICKER, TICKER_FUTURES, TICKER_SWAP, OPEN_INTEREST}:
                for k in ('size', 'amount', 'price', 'timestamp', 'receipt_timestamp', 'bid', 'ask', 'open_interest'):
                    if k in update:
                        update[k] = float(update[k])
                ret.append(update)
            elif dtype == FUNDING:
                for k in update:
                    try:
                        update[k] = float(update[k])
                    except:
                        pass
                ret.append(update)
            self.ids[key].append(update_id)

        self.last_id[key] = self.ids[key][-1]
        return ret

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
