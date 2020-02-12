'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import json
import itertools
import logging

import requests

from cryptostore.data.store import Store


LOG = logging.getLogger('cryptostore')


def chunk(iterable, length):
    return (iterable[i : i + length] for i in range(0, len(iterable), length))


class ElasticSearch(Store):
    def __init__(self, config: dict):
        self.data = None
        self.host = config.host
        self.user = config.user
        self.token = config.token
        self.settings = {'settings': {
                            "index" : {
                                "number_of_shards" : config.shards,
                                "number_of_replicas" : config.replicas,
                                "refresh_interval": config.refresh_interval
                                }
                            }
                        }

    def aggregate(self, data):
        self.data = data

    def write(self, exchange, data_type, pair, timestamp):
        if requests.head(f"{self.host}/{data_type}").status_code != 200:
            r = requests.put(f"{self.host}/{data_type}", data=json.dumps(self.settings), auth=(self.user, self.token), headers={'content-type': 'application/json'})
            if r.status_code != 200:
                LOG.error("Elasticsearch Index creation failed: %s", r.text)
            r.raise_for_status()

        LOG.info("Writing %d documents to Elasticsearch", len(self.data))
        for c in chunk(self.data, 10000):
            data = itertools.chain(*zip(['{"index": {}}'] * len(c), [json.dumps(d) for d in c]))
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
