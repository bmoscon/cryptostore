'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from cryptofeed. defines import BID, ASK


def book_flatten(book: dict, timestamp: float, delta: str) -> dict:
    """
    takes book and returns a list of dict, where each element in the list
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
                    ret.append({'side': side, 'price': price, 'size': size, 'order_id': order_id, 'timestamp': timestamp, 'delta': delta})
            else:
                ret.append({'side': side, 'price': price, 'size': data, 'timestamp': timestamp, 'delta': delta})
    return ret