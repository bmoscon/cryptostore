'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import json

import zmq


def receiver(port):
    addr = 'tcp://127.0.0.1:{}'.format(port)
    ctx = zmq.Context.instance()
    s = ctx.socket(zmq.SUB)
    # empty subscription for all data, otherwise specify exchange-type-symbol
    s.setsockopt(zmq.SUBSCRIBE, b'COINBASE-book-BTC-USD')

    s.bind(addr)
    while True:
        # Try/Except can be omitted if zmq.NOBLOCK flag is removed
        try:
            data = s.recv_string(zmq.NOBLOCK)
        except zmq.error.Again:
            continue
        key, msg = data.split(" ", 1)
        print(key)
        print(json.loads(msg))


def main():
    receiver(5678)


if __name__ == '__main__':
    main()
