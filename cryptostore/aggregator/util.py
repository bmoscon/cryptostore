'''
Copyright (C) 2018-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from typing import Tuple, Generator
import json
from cryptofeed. defines import BID, ASK


def safe_float(item):
    try:
        return float(item)
    except Exception:
        return float('nan')


def book_flatten(book: dict, timestamp: float, receipt_timestamp: float, delta: str) -> list:
    """
    Takes book and returns a list of dict, where each element in the list
    is a dictionary with a single row of book data.

    eg.
    L2:
    [{'side': str, 'price': float, 'size': float, 'timestamp': float}, {...}, ...]

    L3:
    [{'side': str, 'price': float, 'size': float, 'timestamp': float, 'order_id': str}, {...}, ...]
    """
    ret = []
    for side in (BID, ASK):
        for price, data in book[side].items():
            if isinstance(data, dict):
                # L3 book
                for order_id, size in data.items():
                    ret.append({'side': side, 'price': price, 'size': size, 'order_id': order_id, 'timestamp': timestamp, 'receipt_timestamp': receipt_timestamp, 'delta': delta})
            else:
                ret.append({'side': side, 'price': price, 'size': data, 'timestamp': timestamp, 'receipt_timestamp': receipt_timestamp, 'delta': delta})
    return ret


def l2_book_flatten(data: Tuple[dict]) -> Tuple[tuple, Generator]:
    """
    Take a tuple of dict (containing nested dict), and returns a generator
    yielding flattened dicts, i.e. where each element is a dictionary with a
    single row of book data.

    Returns:
      -  data (Generator[dict]):
         L2 book generator:
         ({'side': str, 'price': float, 'size': float, 'timestamp': float, 'receipt_timestamp': float}, {...}, ...)
      -  keys (tuple):
         Keys of the dictionaries are also returned in a tuple to prevent having to touch the generator only for
         having this information (is used for instance for `parquet` and `artctic` backends).
    """
    keys = ('timestamp', 'receipt_timestamp', 'delta', 'side', 'price', 'size')

    def _parse(data):
        for trans in data:
            delta = True if 'delta' in trans else False
            ts, r_ts, _d = trans['timestamp'], trans['receipt_timestamp'], json.loads(trans['delta' if delta else 'book'])
            res = {'timestamp': safe_float(ts), 'receipt_timestamp': safe_float(r_ts), 'delta': delta}
            for side in (BID, ASK):
                res['side'] = side
                if delta:
                    for price, size in _d[side]:
                        res['price'], res['size'] = float(price), float(size)
                        yield res
                else:
                    for price, size in _d[side].items():
                        res['price'], res['size'] = float(price), float(size)
                        yield res

    return keys, _parse(data)


def l3_book_flatten(data: Tuple[dict]) -> Tuple[tuple, Generator]:
    """
    Take a tuple of dict (containing nested dict), and returns a generator
    yielding flattened dicts, i.e. where each element is a dictionary with a
    single row of book data.

    Returns:
      -  data (Generator[dict]):
         L3 book generator:
         ({'side': str, 'price': float, 'size': float, 'timestamp': float, 'receipt_timestamp': float, 'order_id': str}, {...}, ...)
      -  keys (tuple):
         Keys of the dictionaries are also returned in a tuple to prevent having to touch the generator only for
         having this information (is used for instance for `parquet` and `artctic` backends).
    """
    keys = ('timestamp', 'receipt_timestamp', 'delta', 'side', 'price', 'size', 'order_id')

    def _parse(data):
        for trans in data:
            delta = True if 'delta' in trans else False
            ts, r_ts, _d = trans['timestamp'], trans['receipt_timestamp'], json.loads(trans['delta' if delta else 'book'])
            res = {'timestamp': safe_float(ts), 'receipt_timestamp': safe_float(r_ts), 'delta': delta}
            for side in (BID, ASK):
                res['side'] = side
                if delta:
                    for order_id, price, size in _d[side]:
                        res['price'], res['size'], res['order_id'] = float(price), float(size), order_id
                        yield res
                else:
                    for price, dat in _d[side].items():
                        for order_id, size in dat.items():
                            res['price'], res['size'], res['order_id'] = float(price), float(size), order_id
                            yield res
    return keys, _parse(data)
