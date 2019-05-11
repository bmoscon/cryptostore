'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import json

from kafka import KafkaConsumer, TopicPartition
from confluent_kafka.admin import AdminClient
import logging
from collections import defaultdict
from cryptofeed.defines import L2_BOOK, L3_BOOK, BID, ASK

from cryptostore.aggregator.cache import Cache


LOG = logging.getLogger('cryptostore')


class Kafka(Cache):
    def __init__(self, ip, port, del_after_read=True, flush=False):
        self.del_after_read = del_after_read
        self.conn = {}
        self.ip = ip
        self.port = port
        self.del_after_read

        if flush:
            ac = AdminClient({'bootstrap.servers': f"{ip}:{port}"})
            topics = list(ac.list_topics().topics.keys())
            for topic, status in ac.delete_topics(topics).items():
                try:
                    status.result()  
                    LOG.info("Topic %s deleted", topic)
                except Exception as e:
                    LOG.warning("Failed to delete topic %s: %s", topic, e)

    def _conn(self, key):
        if key not in self.conn:
            self.conn[key] = KafkaConsumer(key,
                                           bootstrap_servers=f"{self.ip}:{self.port}",
                                           client_id=f'cryptostore-{key}',
                                           enable_auto_commit=self.del_after_read,
                                           group_id=f'cryptofeed-{key}',
                                           max_poll_records=100000)
        LOG.info("Next message offset %s", self.conn[key].end_offsets([TopicPartition(topic=key, partition=0)]))
        return self.conn[key]

    def read(self, exchange, dtype, pair):
        key = f'{dtype}-{exchange}-{pair}'

        data = self._conn(key).poll()
        print(len(data))
        ret = []
        for _, records in data.items():
            print(len(records))
            for entry in records:
                ret.append(json.loads(entry.value.decode('utf8')))
        if dtype in {L2_BOOK, L3_BOOK}:
            d = []
            for book in ret:
                ts = book['timestamp']
                for side in (BID, ASK):
                    for price, data in book[side].items():
                        if isinstance(data, dict):
                            # L3 book
                            for order_id, size in data.items():
                                d.append({'side': side, 'price': price, 'size': size, 'order_id': order_id, 'timestamp': ts})
                        else:
                            d.append({'side': side, 'price': price, 'size': data, 'timestamp': ts})                
            ret = d

        return ret

    def delete(self, exchange, dtype, pair):
        # kafka handles this internally if not disabled
        return