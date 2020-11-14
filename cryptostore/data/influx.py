'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from decimal import Decimal
from collections import defaultdict
import logging

from cryptofeed.defines import TRADES, L2_BOOK, L3_BOOK, TICKER, FUNDING, OPEN_INTEREST, LIQUIDATIONS
import requests

from cryptostore.exceptions import EngineWriteError
from cryptostore.data.store import Store


LOG = logging.getLogger('cryptostore')


def chunk(iterable, length):
    return (iterable[i : i + length] for i in range(0, len(iterable), length))


class InfluxDB(Store):
    def __init__(self, config: dict):
        self.data = None
        self.host = config.host
        self.db = config.db
        self.username = config.username if "username" in config else None
        self.password = config.password if "password" in config else None
        self.addr = f"{config.host}/write?db={config.db}&u={self.username}&p={self.password}"
        if 'create' in config and config.create:
            r = requests.post(f'{config.host}/query?u={self.username}&p={self.password}', data={'q': f'CREATE DATABASE {config.db}'})
            r.raise_for_status()

    def aggregate(self, data):
        if isinstance(data[0], dict):
            # Case `data` is a list or tuple of dict.
            self.data = data
        else:
            # Case `data` is a tuple with tuple of keys of dict as 1st parameter,
            # and generator of dicts as 2nd paramter.
            # Data is transformed back into a list of dict
            self.data = data[1]

    def write(self, exchange, data_type, pair, timestamp):
        if not self.data:
            return
        agg = []
        # influx cant handle duplicate data (?!) so we need to
        # incremement timestamps on data that have the same timestamp
        used_ts = set()
        if data_type == TRADES:
            for entry in self.data:
                ts = int(Decimal(entry["timestamp"]) * 1000000000)
                while ts in used_ts:
                    ts += 1
                used_ts.add(ts)
                if 'id' in entry:
                    agg.append(f'{data_type}-{exchange},pair={pair},exchange={exchange} side="{entry["side"]}",id="{entry["id"]}",amount={entry["amount"]},price={entry["price"]},timestamp={entry["timestamp"]},receipt_timestamp={entry["receipt_timestamp"]} {ts}')
                else:
                    agg.append(f'{data_type}-{exchange},pair={pair},exchange={exchange} side="{entry["side"]}",amount={entry["amount"]},price={entry["price"]},timestamp={entry["timestamp"]},receipt_timestamp={entry["receipt_timestamp"]} {ts}')
        elif data_type == TICKER:
            for entry in self.data:
                ts = int(Decimal(entry["timestamp"]) * 1000000000)
                agg.append(f'{data_type}-{exchange},pair={pair},exchange={exchange} bid={entry["bid"]},ask={entry["ask"]},timestamp={entry["timestamp"]},receipt_timestamp={entry["receipt_timestamp"]} {ts}')

        elif data_type == L2_BOOK:
            for entry in self.data:
                ts = int(Decimal(entry["timestamp"]) * 1000000000)
                while ts in used_ts:
                    ts += 1
                used_ts.add(ts)

                agg.append(f'{data_type}-{exchange},pair={pair},exchange={exchange},delta={entry["delta"]} side="{entry["side"]}",timestamp={entry["timestamp"]},receipt_timestamp={entry["receipt_timestamp"]},price={entry["price"]},amount={entry["size"]} {ts}')
        elif data_type == L3_BOOK:
            for entry in self.data:
                ts = int(Decimal(entry["timestamp"]) * 1000000000)
                while ts in used_ts:
                    ts += 1
                used_ts.add(ts)

                agg.append(f'{data_type}-{exchange},pair={pair},exchange={exchange},delta={entry["delta"]} side="{entry["side"]}",id="{entry["order_id"]}",timestamp={entry["timestamp"]},receipt_timestamp={entry["receipt_timestamp"]},price="{entry["price"]}",amount="{entry["size"]}" {ts}')
                ts += 1
        elif data_type == FUNDING:
            for entry in self.data:
                formatted = [f"{key}={value}" for key, value in entry.items() if isinstance(value, float)]
                formatted = ','.join(formatted + [f'{key}="{value}"' for key, value in entry.items() if not isinstance(value, float)])
                agg.append(f'{data_type}-{exchange},pair={pair},exchange={exchange} {formatted}')
        elif data_type == OPEN_INTEREST:
            for entry in self.data:
                ts = int(Decimal(entry["timestamp"]) * 1000000000)
                agg.append(f'{data_type}-{exchange},pair={pair},exchange={exchange} open_interest={entry["open_interest"]},timestamp={entry["timestamp"]},receipt_timestamp={entry["receipt_timestamp"]} {ts}')
        elif data_type == LIQUIDATIONS:
            for entry in self.data:
                ts = int(Decimal(entry["timestamp"]) * 1000000000)
                agg.append(f'{data_type}-{exchange},pair={pair},exchange={exchange} side="{entry["side"]}",leaves_qty={entry["leaves_qty"]},order_id="{entry["order_id"]}",price={entry["price"]},timestamp={entry["timestamp"]},receipt_timestamp={entry["receipt_timestamp"]} {ts}')

        # https://v2.docs.influxdata.com/v2.0/write-data/best-practices/optimize-writes/
        # Tuning docs indicate 5k is the ideal chunk size for batch writes
        for c in chunk(agg, 5000):
            c = '\n'.join(c)
            r = requests.post(self.addr, data=c)
            # per influx docs, returns 204 on success
            if r.status_code != 204:
                LOG.error("Influx: Failed to write data to %s - %d:%s", self.addr, r.status_code, r.reason)
                raise EngineWriteError
        self.data = None

    def get_start_date(self, exchange: str, data_type: str, pair: str) -> float:
        try:
            r = requests.get(f"{self.host}/query?db={self.db}&u={self.username}&p={self.password}", params={'q': f'SELECT first(timestamp) from "{data_type}-{exchange}" where pair=\'{pair}\''})
            return r.json()['results'][0]['series'][0]['values'][0][1]
        except Exception:
            return None
