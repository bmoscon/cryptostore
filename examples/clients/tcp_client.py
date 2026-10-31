'''
Copyright (C) 2018-2026 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.


Reads frames from a cryptostore stream backend over TCP or a unix socket.

    python tcp_client.py                       # tcp://127.0.0.1:8080
    python tcp_client.py 10.0.0.5:8080
    python tcp_client.py /data/cryptostore.sock

Each line is one JSON frame: {"type": "<channel>", "data": {...}} - the same format cryptofeed's
socket backend produces. Prices and sizes are exact decimals on the wire; parse_float=Decimal keeps
them exact. A book frame carries either "book" (a full snapshot) or "delta" (changed levels, size 0 = removed).
'''
import asyncio
import json
import sys
from decimal import Decimal


async def main(target: str):
    if target.startswith('/'):
        reader, _ = await asyncio.open_unix_connection(target)
    else:
        host, _, port = target.rpartition(':')
        reader, _ = await asyncio.open_connection(host or '127.0.0.1', int(port))

    books = {}
    async for line in reader:
        frame = json.loads(line, parse_float=Decimal)
        kind, data = frame['type'], frame['data']
        key = (data['exchange'], data['symbol'])

        if kind == 'trades':
            print(f"{data['exchange']:<10} {data['symbol']:<12} {data['side']:<4} {data['amount']} @ {data['price']}")
        elif kind in ('l2_book', 'l3_book'):
            if 'book' in data:
                books[key] = {side: dict(levels) for side, levels in data['book'].items()}
                print(f"{data['exchange']:<10} {data['symbol']:<12} snapshot {len(books[key]['bid'])} bids / {len(books[key]['ask'])} asks")
            elif key in books:
                for side, levels in data['delta'].items():
                    for price, size in levels:
                        if Decimal(str(size)) == 0:
                            books[key][side].pop(str(price), None)
                        else:
                            books[key][side][str(price)] = size
        else:
            print(f'{kind}: {data}')


if __name__ == '__main__':
    try:
        asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else '127.0.0.1:8080'))
    except KeyboardInterrupt:
        pass
