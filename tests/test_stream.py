'''
Copyright (C) 2018-2026 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import asyncio
import os
import tempfile
from decimal import Decimal

import pytest
from cryptofeed import _json as json
from cryptofeed.defines import ASK, BID, BUY, L2_BOOK, TRADES
from cryptofeed.types import OrderBook, Trade

from cryptostore.stream import BookStreamCallback, StreamServer, TradeStream, parse_tcp


PRICE = Decimal('65432.12')
RECEIPT = 1786310525.5


def trades(n):
    return [Trade('TESTEX', 'BTC-USD', BUY, Decimal('0.1') + i, PRICE + i, 1786310525.0 + i, id=str(i)) for i in range(n)]


async def run_backend(backend, objects):
    async with asyncio.TaskGroup() as tg:
        backend.start_writer(tg, name='test.stream')
        for i, obj in enumerate(objects):
            await backend(obj, RECEIPT + i)
        await backend.stop()
    return backend.stats


async def read_frames(reader, count, deadline=5.0):
    frames = []
    async with asyncio.timeout(deadline):
        while len(frames) < count:
            line = await reader.readline()
            if not line:
                break
            frames.append(json.loads(line, parse_float=Decimal))
    return frames


def test_parse_tcp():
    assert parse_tcp(':8080') == ('0.0.0.0', 8080)
    assert parse_tcp('127.0.0.1:9') == ('127.0.0.1', 9)
    assert parse_tcp(5555) == ('0.0.0.0', 5555)
    assert parse_tcp('[::1]:80') == ('::1', 80)
    for bad in ('x', 'host:abc', '1.2.3.4:70000', True, None):
        with pytest.raises(ValueError):
            parse_tcp(bad)


def test_server_rejects_bad_options():
    with pytest.raises(ValueError, match='at least one'):
        StreamServer()
    with pytest.raises(ValueError, match='client_queue'):
        StreamServer(tcp=':0', client_queue=0)
    with pytest.raises(ValueError, match='zmq must be'):
        StreamServer(zmq='5555')
    with pytest.raises(ValueError, match='limited to 103'):
        StreamServer(uds='/' + 'x' * 110)


async def test_tcp_clients_receive_frames_in_order():
    server = StreamServer(tcp='127.0.0.1:0')
    await server.start()
    try:
        port = server.tcp_port
        reader_a, writer_a = await asyncio.open_connection('127.0.0.1', port)
        reader_b, writer_b = await asyncio.open_connection('127.0.0.1', port)
        await asyncio.sleep(0.05)
        assert server.clients == 2

        backend = TradeStream(server, key=TRADES)
        stats = await run_backend(backend, trades(5))
        assert stats.written == 5 and stats.dropped_failed == 0 and stats.last_error is None

        for reader in (reader_a, reader_b):
            frames = await read_frames(reader, 5)
            assert [frame['type'] for frame in frames] == ['trades'] * 5
            assert [frame['data']['id'] for frame in frames] == ['0', '1', '2', '3', '4']
            assert frames[0]['data']['price'] == PRICE            # Decimal survives the wire as an exact number
            assert frames[0]['data']['receipt_timestamp'] == Decimal(repr(RECEIPT))
            assert frames[0]['data']['exchange'] == 'TESTEX'
        assert server.published == 5

        writer_a.close()
        await writer_a.wait_closed()
        await asyncio.sleep(0.05)
        assert server.clients == 1
        writer_b.close()
    finally:
        await server.close()
    assert server.clients == 0


async def test_unix_socket_and_book_snapshots():
    with tempfile.TemporaryDirectory() as directory:
        path = os.path.join(directory, 'cs.sock')
        server = StreamServer(uds=path)
        await server.start()
        try:
            reader, writer = await asyncio.open_unix_connection(path)
            await asyncio.sleep(0.05)

            book = OrderBook('TESTEX', 'BTC-USD', bids={PRICE: Decimal('1')}, asks={PRICE + 1: Decimal('2')})
            book.timestamp = 1786310525.0
            delta = OrderBook('TESTEX', 'BTC-USD', bids={PRICE: Decimal('1'), PRICE - 1: Decimal('3')}, asks={PRICE + 1: Decimal('2')})
            delta.timestamp = 1786310526.0
            delta.delta = {BID: [(PRICE - 1, Decimal('3'))], ASK: []}

            backend = BookStreamCallback(server, key=L2_BOOK, snapshot_interval=1)
            await run_backend(backend, [book, delta])

            frames = await read_frames(reader, 3)
            assert [frame['type'] for frame in frames] == ['l2_book'] * 3
            assert 'delta' not in frames[0]['data'] and 'book' in frames[0]['data']       # snapshot
            assert 'delta' in frames[1]['data']                                           # the delta
            assert 'delta' not in frames[2]['data']                                       # snapshot_interval=1 forces a snapshot after it
            assert frames[1]['data']['delta']['bid'] == [[PRICE - 1, Decimal('3')]]
            writer.close()
        finally:
            await server.close()
        assert not os.path.exists(path)  # noqa: ASYNC240


async def test_slow_client_is_disconnected_not_fed_a_gap():
    server = StreamServer(tcp='127.0.0.1:0', client_queue=3)
    await server.start()
    try:
        reader, writer = await asyncio.open_connection('127.0.0.1', server.tcp_port)
        await asyncio.sleep(0.05)
        # fill the client's queue without yielding, so the pump never gets a chance to drain it
        client = next(iter(server._clients))
        for i in range(3):
            client.queue.put_nowait(b'{"type":"x"}\n')
        await server.publish('TESTEX-trades-BTC-USD', b'{"type":"trades"}')   # fourth frame overflows
        assert server.kicked == 1
        assert server.clients == 0
        data = await asyncio.wait_for(reader.read(), 5)
        assert data == b'' or data.count(b'\n') <= 3
        writer.close()
    finally:
        await server.close()


async def test_zmq_pub_bind():
    zmq = pytest.importorskip('zmq')
    zmq_asyncio = pytest.importorskip('zmq.asyncio')

    server = StreamServer(zmq='tcp://127.0.0.1:0')
    await server.start()
    try:
        endpoint = server._pub.getsockopt_string(zmq.LAST_ENDPOINT)
        ctx = zmq_asyncio.Context()
        sub = ctx.socket(zmq.SUB)
        sub.connect(endpoint)
        sub.setsockopt_string(zmq.SUBSCRIBE, 'TESTEX-trades-')
        await asyncio.sleep(0.2)   # slow joiner

        backend = TradeStream(server, key=TRADES)
        await run_backend(backend, trades(3))

        async with asyncio.timeout(5):
            message = await sub.recv_string()
        topic, payload = message.split(' ', 1)
        assert topic == 'TESTEX-trades-BTC-USD'
        frame = json.loads(payload, parse_float=Decimal)
        assert frame['type'] == 'trades' and frame['data']['id'] == '0'
        sub.close(linger=0)
        ctx.term()
    finally:
        await server.close()


async def test_publish_from_another_thread():
    '''a worker on its own loop hands frames to the server, which fans them out on the loop it was started on'''
    server = StreamServer(tcp='127.0.0.1:0')
    await server.start()
    try:
        reader, writer = await asyncio.open_connection('127.0.0.1', server.tcp_port)
        await asyncio.sleep(0.05)

        def worker():
            async def produce():
                return await run_backend(TradeStream(server, key=TRADES), trades(4))
            with asyncio.Runner() as runner:
                return runner.run(produce())

        stats = await asyncio.to_thread(worker)
        assert stats.written == 4 and stats.last_error is None
        frames = await read_frames(reader, 4)
        assert [frame['data']['id'] for frame in frames] == ['0', '1', '2', '3']
        assert server.published == 4
        writer.close()
    finally:
        await server.close()
