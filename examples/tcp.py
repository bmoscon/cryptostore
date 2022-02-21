'''
Copyright (C) 2018-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.


Run this file, then start the docker container with


docker run -e EXCHANGE='COINBASE' \
           -e CHANNELS='trades' \
           -e SYMBOLS='BTC-USD' \
           -e BACKEND='TCP' \
           -e HOST='tcp://127.0.0.1' \
           -e PORT=8080 \
           cryptostore:latest
'''
import asyncio
from decimal import Decimal
import json


async def reader(reader, writer):
    while True:
        data = await reader.read(1024 * 640)
        message = data.decode()
        # if multiple messages are received back to back,
        # need to make sure they are formatted as if in an array
        message = message.replace("}{", "},{")
        message = f"[{message}]"
        message = json.loads(message, parse_float=Decimal)

        addr = writer.get_extra_info('peername')

        print(f"Received {message!r} from {addr!r}")


async def main():
    server = await asyncio.start_server(reader, '127.0.0.1', 8080)
    await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(main())
