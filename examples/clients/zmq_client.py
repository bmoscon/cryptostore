'''
Copyright (C) 2018-2026 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.


Subscribes to a cryptostore stream backend's ZeroMQ PUB endpoint.

    python zmq_client.py                          # everything on tcp://127.0.0.1:5555
    python zmq_client.py COINBASE-trades-         # only Coinbase trades
    python zmq_client.py COINBASE-l2_book-BTC-USD tcp://10.0.0.5:5555

Messages are "<EXCHANGE>-<channel>-<SYMBOL> <json>", the format cryptofeed's own ZeroMQ backend
uses, so the topic prefix is the subscription filter.
'''
import json
import sys
from decimal import Decimal

import zmq


def main(prefix: str, endpoint: str):
    ctx = zmq.Context()
    sub = ctx.socket(zmq.SUB)
    sub.connect(endpoint)
    sub.setsockopt_string(zmq.SUBSCRIBE, prefix)
    print(f'subscribed to {prefix!r} on {endpoint}')
    while True:
        topic, payload = sub.recv_string().split(' ', 1)
        frame = json.loads(payload, parse_float=Decimal)
        print(topic, frame['data'])


if __name__ == '__main__':
    args = sys.argv[1:]
    try:
        main(args[0] if args else '', args[1] if len(args) > 1 else 'tcp://127.0.0.1:5555')
    except KeyboardInterrupt:
        pass
