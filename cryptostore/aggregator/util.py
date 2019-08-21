'''
Copyright (C) 2018-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from cryptofeed. defines import BID, ASK
from collections import OrderedDict


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

def book_wide(data: list):
    ret = []
    od = OrderedDict({})
    for dic in data:
        od.update({'timestamp': dic['timestamp']})
        for index, i in enumerate(zip(dic['ask'].items(), sorted(dic['bid'].items(), reverse=True))):
            od.update({f'asks[{index}].price': float(f'{i[0][0]}')})
            od.update({f'asks[{index}].size': float(f'{i[0][1]}')})
            od.update({f'bids[{index}].price': float(f'{i[1][0]}')})
            od.update({f'bids[{index}].size': float(f'{i[1][1]}')})
        ret.append(od.copy())
    return ret
