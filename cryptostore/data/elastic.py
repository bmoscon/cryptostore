'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import json
import itertools
import logging

import requests

from cryptostore.data.store import Store


LOG = logging.getLogger('cryptostore')


SETTINGS = {
        "index" : {
            "number_of_shards" : 5, 
            "number_of_replicas" : 0
        }
    }
MAPPINGS = {'trades': {
                "settings": SETTINGS,
                "mappings":{
                    "properties":{
                        "feed": {"type": "keyword"},
                        "pair": {"type": "keyword"},
                        "timestamp": {"type": "double"},
                        "amount": {"type": "double"},
                        "price": {"type":"double"},
                        "id": {"type": "text"},
                        "side": {"type": "text"}
                    }
                }
            },
            'l2_book': {
                "settings": SETTINGS,
                "mappings":{
                    "properties":{
                        "feed": {"type": "keyword"},
                        "pair": {"type": "keyword"},
                        "timestamp": {"type": "double"},
                        "size": {"type": "double"},
                        "price": {"type":"double"},
                        "side": {"type": "text"},
                        "delta": {"type": "boolean"}
                    }
                }
            },
            'l3_book': {
                "settings": SETTINGS,
                "mappings":{
                    "properties":{
                        "feed": {"type": "keyword"},
                        "pair": {"type": "keyword"},
                        "timestamp": {"type": "double"},
                        "size": {"type": "double"},
                        "price": {"type":"double"},
                        "side": {"type": "text"},
                        "order_id": {"type": "text"},
                        "delta": {"type": "boolean"}
                    }
                }
            }
        }

def chunk(iterable, length):
    return (iterable[i : i + length] for i in range(0, len(iterable), length))


class ElasticSearch(Store):
    def __init__(self, config: dict):
        self.data = None
        self.host = config.host
        self.user = config.user
        self.token = config.token

    def aggregate(self, data):
        self.data = data

    def write(self, exchange, data_type, pair, timestamp):
        if requests.head(f"{self.host}/{data_type}").status_code != 200:
            r = requests.put(f"{self.host}/{data_type}", data=json.dumps(MAPPINGS[data_type]), auth=(self.user, self.token), headers={'content-type': 'application/json'})
            if r.status_code != 200:
                LOG.error("Elasticsearch Index creation failed: %s", r.text)
            r.raise_for_status()

        for c in chunk(self.data, 100000):
            data = itertools.chain(*zip([json.dumps({'index': {}})] * len(c), [json.dumps(d) for d in c]))
            data = '\n'.join(data)
            data = f"{data}\n"
            r = requests.post(f"{self.host}/{data_type}/{data_type}/_bulk", auth=(self.user, self.token), data=data, headers={'content-type': 'application/x-ndjson'})
            if r.status_code != 200:
                LOG.error("Elasticsearch insert failed: %s", r.text)
            r.raise_for_status()
        self.data = None

    def get_start_date(self, exchange: str, data_type: str, pair: str) -> float:
        try:
            data = {
                "query":{
                    "bool": {
                        "must": [{
                            "match_phrase": {"feed": exchange}
                        },{
                            "match_phrase": {"pair": pair}
                        }]
                    }
                },
                "aggs" : {
                    "min_timestamp" : { "min" : { "field" : "timestamp" }}}
            }
            r = requests.post(f"{self.host}/{data_type}/{data_type}/_search?size=0", auth=(self.user, self.token), data=json.dumps(data), headers={'content-type': 'application/json'})
            return r.json()['aggregations']['min_timestamp']['value']
        except Exception:
            return None
