'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from decimal import Decimal
from collections import defaultdict

from cryptofeed.defines import TRADES, L2_BOOK, L3_BOOK
import requests

from cryptostore.data.store import Store

def chunk(iterable, length):
    return (iterable[i : i + length] for i in range(0, len(iterable), length))

class InfluxDB(Store):
    def __init__(self, config: dict):
        self.data = None
        self.host = config.host
        self.db = config.db
        self.addr = f"{config.host}/write?db={config.db}"
        if 'create' in config and config.create:
            r = requests.post(f'{config.host}/query', data={'q': f'CREATE DATABASE {config.db}'})
            if r.status_code != 200:
                r.raise_for_status()

    def aggregate(self, data):
        self.data = data

    def write(self, exchange, data_type, pair, timestamp):
        agg = []
        # influx cant handle duplicate data (?!) so we need to
        # incremement timestamps on data that have the same timestamp
        used_ts = defaultdict(set)
        if data_type == TRADES:
            for entry in self.data:
                ts = int(Decimal(entry["timestamp"]) * 1000000000)
                while ts in used_ts[pair]:
                    ts += 1
                used_ts[pair].add(ts)
                agg.append(f'{data_type}-{exchange},pair={pair} side="{entry["side"]}",id="{entry["id"]}",amount={entry["amount"]},price={entry["price"]},timestamp={entry["timestamp"]} {ts}')
        elif data_type == L2_BOOK:
            if len(self.data):
                ts = int(Decimal(self.data[0]["timestamp"]) * 1000000000)
            for entry in self.data:
                agg.append(f'{data_type}-{exchange},pair={pair},delta={entry["delta"]} side="{entry["side"]}",timestamp={entry["timestamp"]},price={entry["price"]},amount={entry["size"]} {ts}')
                ts += 1
        elif data_type == L3_BOOK:
            if len(self.data):
                ts = int(Decimal(self.data[0]["timestamp"]) * 1000000000)
            for entry in self.data:
                agg.append(f'{data_type}-{exchange},pair={pair},delta={entry["delta"]} side="{entry["side"]}",id="{entry["order_id"]}",timestamp={entry["timestamp"]},price="{entry["price"]}",amount="{entry["size"]}" {ts}')
                ts += 1
        
        for c in chunk(agg, 100000):
            c = '\n'.join(c)
            r = requests.post(self.addr, data=c)
            r.raise_for_status()
        self.data = None

    def get_start_date(self, exchange: str, data_type: str, pair: str) -> float:
        try:
            r = requests.get(f"{self.host}/query?db={self.db}", params={'q': f'SELECT first(timestamp) from "{data_type}-{exchange}" where pair=\'{pair}\''})
            return r.json()['results'][0]['series'][0]['values'][0][1]
        except Exception:
            return None
