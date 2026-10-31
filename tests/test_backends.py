'''
Copyright (C) 2018-2026 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from decimal import Decimal

import pytest
from cryptofeed.backends.backend import BackendCallback, BackendQueue
from cryptofeed.defines import CANDLES, FUNDING, INDEX, L1_BOOK, L2_BOOK, L3_BOOK, LIQUIDATIONS, OPEN_INTEREST, TICKER, TRADES

from cryptostore.backends import BACKEND_TYPES, BackendBuilder, BackendError, BackendType
from cryptostore.config import parse


def test_registry_covers_every_cryptofeed_backend():
    assert {'redis', 'redis_stream', 'mongo', 'postgres', 'influxdb', 'questdb', 'kafka', 'zmq', 'socket', 'stream', 'tty'} <= set(BACKEND_TYPES)
    assert BACKEND_TYPES['stdout'] is BACKEND_TYPES['tty']
    for backend_type in BACKEND_TYPES.values():
        assert isinstance(backend_type, BackendType)
        assert backend_type.channels(), backend_type.name


def test_postgres_has_no_generic_fallback():
    postgres = BACKEND_TYPES['postgres']
    assert postgres.supports(INDEX)
    assert postgres.supports(L2_BOOK)
    assert not postgres.supports(L1_BOOK)


@pytest.mark.parametrize('name', ['socket', 'zmq', 'redis', 'redis_stream', 'kafka', 'mongo', 'influxdb', 'questdb', 'stream'])
def test_every_channel_resolves_to_a_class(name):
    backend_type = BACKEND_TYPES[name]
    pytest.importorskip(backend_type.module.split('.')[0] if backend_type.module.startswith('cryptostore') else _dependency(backend_type))
    for channel in (TRADES, TICKER, FUNDING, OPEN_INTEREST, LIQUIDATIONS, CANDLES, INDEX, L1_BOOK, L2_BOOK, L3_BOOK):
        cls = backend_type.backend_class(channel)
        assert issubclass(cls, BackendQueue), (name, channel)
        if channel not in (L2_BOOK, L3_BOOK) and name != 'questdb':
            assert issubclass(cls, BackendCallback)
            assert cls.default_key


def _dependency(backend_type: BackendType) -> str:
    return {'redis': 'redis', 'redis_stream': 'redis', 'kafka': 'aiokafka', 'mongo': 'pymongo', 'zmq': 'zmq'}.get(backend_type.name, 'cryptofeed')


def test_synthesized_class_matches_cryptofeed_pattern():
    from cryptofeed.backends.socket import SocketCallback, TradeSocket
    cls = BACKEND_TYPES['socket'].backend_class(L1_BOOK)
    assert cls.__name__ == 'L1BookSocket'
    assert cls.__bases__ == (SocketCallback, BackendCallback)
    assert cls.default_key == 'l1_book'
    # a second lookup of a hand-written class returns cryptofeed's own
    assert BACKEND_TYPES['socket'].backend_class(TRADES) is TradeSocket


def test_builder_shares_one_instance_per_backend_and_channel(base_config):
    base_config['backends']['sock'] = {'type': 'socket', 'addr': 'tcp://127.0.0.1', 'port': 9999, 'numeric_type': 'decimal',
                                    'book': {'snapshot_interval': 7}, 'queue': {'max_depth': 42}}
    base_config['exchanges']['KRAKEN'] = {'channels': {'trades': ['BTC-USD'], 'l2_book': ['BTC-USD']}}
    config = parse(base_config)
    builder = BackendBuilder(config.backends)

    trades = builder.writer('sock', TRADES)
    assert builder.writer('sock', TRADES) is trades
    assert trades.numeric_type is Decimal
    assert trades.max_depth == 42
    assert trades.port == 9999

    book = builder.writer('sock', L2_BOOK)
    assert book is not trades
    assert book.snapshot_interval == 7
    assert book.max_depth == 42
    assert set(builder.writers()) == {('sock', TRADES), ('sock', L2_BOOK)}


def test_builder_stream_backend_shares_a_server(base_config):
    base_config['backends']['stream'] = {'type': 'stream', 'tcp': ':0', 'client_queue': 5, 'numeric_type': 'float', 'book': {'snapshot_interval': 3}}
    config = parse(base_config)
    builder = BackendBuilder(config.backends)
    trades = builder.writer('stream', TRADES)
    book = builder.writer('stream', L2_BOOK)
    assert trades.server is book.server
    assert builder.servers['stream'].client_queue == 5
    assert trades.key == 'trades' and book.key == 'l2_book'
    assert trades.numeric_type is float
    assert book.snapshot_interval == 3


def test_builder_tty(base_config):
    config = parse(base_config)
    builder = BackendBuilder(config.backends)
    tty = builder.writer('out', TRADES)
    assert callable(tty) and not isinstance(tty, BackendQueue)


def test_missing_dependency_is_a_backend_error(monkeypatch):
    backend_type = BackendType(name='fake', module='cryptostore_no_such_module', family='Fake', generic_base='X', extra='fake')
    with pytest.raises(BackendError, match=r'pip install cryptostore\[fake\]'):
        backend_type.backend_class(TRADES)


def test_bad_constructor_argument_is_a_backend_error(base_config):
    base_config['backends']['sock'] = {'type': 'socket', 'addr': 'tcp://127.0.0.1', 'port': 1, 'bogus': 1}
    config = parse(base_config)
    with pytest.raises(BackendError, match="backend 'sock' \\(socket\\)"):
        BackendBuilder(config.backends).writer('sock', TRADES)
