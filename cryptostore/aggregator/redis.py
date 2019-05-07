'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import redis
import logging
from collections import defaultdict

from cryptostore.aggregator.cache import Cache


LOG = logging.getLogger('cryptostore')


class Redis(Cache):
    def __init__(self, ip, port, del_after_read=True, flush=False):
        self.del_after_read = del_after_read
        self.last_id = {}
        self.ids = defaultdict(list)
        self.conn = redis.Redis(ip, port, decode_responses=True)
        if flush:
            LOG.info('Flushing cache')
            self.conn.flushall()


    def read(self, exchange, dtype, pair):
        key = f'{dtype}-{exchange}-{pair}'

        data = self.conn.xread({key: '0-0' if key not in self.last_id else self.last_id[key]})

        if len(data) == 0:
            return []
        else:
            ret = []
            for update_id, update in data[0][1]:
                self.ids[key].append(update_id)
                ret.append(update)

            self.last_id[key] = self.ids[key][-1]
            return ret

    def delete(self, exchange, dtype, pair):
        key = f'{dtype}-{exchange}-{pair}'

        if self.del_after_read:
            self.conn.xdel(key, *self.ids[key])

        self.ids[key] = []
