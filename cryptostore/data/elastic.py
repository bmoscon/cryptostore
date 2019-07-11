'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from decimal import Decimal
import json
import itertools

from cryptofeed.defines import TRADES, L2_BOOK, L3_BOOK
import requests

from cryptostore.data.store import Store


def chunk(iterable, length):
    return (iterable[i : i + length] for i in range(0, len(iterable), length))


class ElasticSearch(Store):
    def __init__(self, config: dict):
        self.data = None
        self.host = config.host

    def aggregate(self, data):
        self.data = data

    def write(self, exchange, data_type, pair, timestamp):
        for c in chunk(self.data, 100000):
            data = itertools.chain(*zip([json.dumps({ "index":{} })] * len(c), [json.dumps(d) for d in c]))
            data = '\n'.join(data)
            data = f"{data}\n"
            r = requests.post(f"{self.host}/{data_type}/{data_type}/_bulk", data=data, headers={'content-type': 'application/x-ndjson'})
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
            r = requests.post(f"{self.host}/{data_type}/{data_type}/_search?size=0", data=json.dumps(data), headers={'content-type': 'application/json'})
            return r.json()['aggregations']['min_timestamp']['value']
        except Exception:
            return None
