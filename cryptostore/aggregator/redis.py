'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import logging
from collections import defaultdict
import json

from cryptofeed.defines import TRADES, L2_BOOK, L3_BOOK

from cryptostore.aggregator.util import book_flatten
from cryptostore.aggregator.cache import Cache
from cryptostore.engines import StorageEngines


LOG = logging.getLogger('cryptostore')


class Redis(Cache):
    def __init__(self, ip, port, del_after_read=True, flush=False):
        self.del_after_read = del_after_read
        self.last_id = {}
        self.ids = defaultdict(list)
        self.conn = StorageEngines.redis.Redis(ip, port, decode_responses=True)
        if flush:
            LOG.info('Flushing cache')
            self.conn.flushall()


    def read(self, exchange, dtype, pair):
        key = f'{dtype}-{exchange}-{pair}'

        data = self.conn.xread({key: '0-0' if key not in self.last_id else self.last_id[key]})

        if len(data) == 0:
            return []

        LOG.info("%s: Read %d messages from Redis", key, len(data[0][1]))
        ret = []
        for update_id, update in data[0][1]:
            if dtype in {L2_BOOK, L3_BOOK}:
                update = json.loads(update['data'])
                update = book_flatten(update, update['timestamp'], update['delta'])
                ret.extend(update)
            if dtype == TRADES:
                ret.append(update)
            self.ids[key].append(update_id)

        self.last_id[key] = self.ids[key][-1]
        return ret

    def delete(self, exchange, dtype, pair):
        key = f'{dtype}-{exchange}-{pair}'

        if self.del_after_read:
            self.conn.xdel(key, *self.ids[key])
        LOG.info("%s: Removed through id %s", key, self.ids[key][-1])
        self.ids[key] = []
