'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import json
import logging

from cryptofeed.defines import L2_BOOK, L3_BOOK, TRADES, TICKER, FUNDING

from cryptostore.engines import StorageEngines
from cryptostore.aggregator.cache import Cache
from cryptostore.aggregator.util import book_flatten


LOG = logging.getLogger('cryptostore')


class Kafka(Cache):
    def __init__(self, ip, port, flush=False):
        self.conn = {}
        self.ip = ip
        self.port = port
        self.ids = {}

        if flush:
            kafka = StorageEngines['confluent_kafka.admin']
            ac = kafka.admin.AdminClient({'bootstrap.servers': f"{ip}:{port}"})
            topics = list(ac.list_topics().topics.keys())
            for topic, status in ac.delete_topics(topics).items():
                try:
                    status.result()
                    LOG.info("Topic %s deleted", topic)
                except Exception as e:
                    LOG.warning("Failed to delete topic %s: %s", topic, e)

    def _conn(self, key):
        if key not in self.conn:
            self.ids[key] = None
            kafka = StorageEngines.confluent_kafka
            self.conn[key] = kafka.Consumer({'bootstrap.servers': f"{self.ip}:{self.port}",
                                             'client.id': f'cryptostore-{key}',
                                             'enable.auto.commit': False,
                                             'group.id': f'cryptofeed-{key}',
                                             'max.poll.interval.ms': 3000000})
            self.conn[key].subscribe([key])
        return self.conn[key]

    def read(self, exchange, dtype, pair):
        key = f'{dtype}-{exchange}-{pair}'
        data = self._conn(key).consume(1000000, timeout=0.5)
        LOG.info("%s: Read %d messages from Kafka", key, len(data))
        ret = []
        for message in data:
            self.ids[key] = message
            update = json.loads(message.value().decode('utf8'))

            if dtype in {L2_BOOK, L3_BOOK}:
                update = book_flatten(update, update['timestamp'], update['delta'])
                ret.extend(update)
            if dtype in {TRADES, TICKER, FUNDING}:
                ret.append(update)
        return ret

    def delete(self, exchange, dtype, pair):
        key = f'{dtype}-{exchange}-{pair}'
        LOG.info("%s: Committing offset %d", key, self.ids[key].offset())
        self._conn(key).commit(message=self.ids[key])
        self.ids[key] = None
