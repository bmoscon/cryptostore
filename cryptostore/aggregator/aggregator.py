'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import asyncio
from multiprocessing import Process
import time
import logging

import redis

from cryptostore.data.storage import Storage
from cryptostore.config import Config


LOG = logging.getLogger('cryptostore')


class Aggregator(Process):
    def __init__(self, config_file=None):
        self.config_file = config_file
        self.last_id = {}
        super().__init__()

    def run(self):
        loop = asyncio.get_event_loop()
        self.config = Config()
        loop.create_task(self.loop())
        loop.run_forever()

    async def loop(self):
        while True:
            delete = self.config.redis['del_after_read']
            r = redis.Redis(self.config.redis['ip'], port=self.config.redis['port'], decode_responses=True)
            for exchange in self.config.exchanges:
                for dtype in self.config.exchanges[exchange]:
                    for pair in self.config.exchanges[exchange][dtype]:
                        key = f'{dtype}-{exchange}-{pair}'
                        store = Storage(self.config)
                        LOG.info(f'Reading {dtype}-{exchange}-{pair}')

                        data = r.xread({key: '0-0' if key not in self.last_id else self.last_id[key]})

                        if len(data) == 0:
                            continue

                        agg = []
                        ids = []
                        for update_id, update in data[0][1]:
                            ids.append(update_id)
                            agg.append(update)

                        self.last_id[key] = ids[-1]

                        store.aggregate(agg)
                        store.write(exchange, dtype, pair, time.time())
                        if delete:
                            r.xdel(f'{dtype}-{exchange}-{pair}', *ids)
            await asyncio.sleep(self.config.storage_interval)
