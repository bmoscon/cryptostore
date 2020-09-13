'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from typing import Tuple, Generator
import json
from cryptofeed. defines import BID, ASK


def book_flatten(book: dict, timestamp: float, receipt_timestamp: float, delta: str) -> dict:
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

    data = map(json.loads, [d['data'] for d in data])
    data = ({'timestamp':float(ts), 'receipt_timestamp':float(r_ts), 'delta': delta, 'side':side,
             'price': float(price), 'size':float(size)} \
            for trans in data                                                                          \
            for ts, r_ts, delta in ((trans['timestamp'], trans['receipt_timestamp'], trans['delta']),) \
            for side in (BID, ASK)                                                                     \
            for price, size in trans[side].items()                                                     )
    keys=('timestamp','receipt_timestamp', 'delta', 'side', 'price', 'size')
    return keys, data


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

    data = map(json.loads, [d['data'] for d in data])
    data = ({'timestamp':float(ts), 'receipt_timestamp':float(r_ts), 'delta': delta, 'side':side,
             'price': float(price), 'size':float(size), 'order_id': order_id} \
            for trans in data                                                                          \
            for ts, r_ts, delta in ((trans['timestamp'], trans['receipt_timestamp'], trans['delta']),) \
            for side in (BID, ASK)                                                                     \
            for price, dat in trans[side].items()                                                      \
            for order_id, size in dat.items()                                                          )
    keys=('timestamp','receipt_timestamp', 'delta', 'side', 'price', 'size', 'order_id')
    return keys, data

