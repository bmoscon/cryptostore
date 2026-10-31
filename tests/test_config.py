'''
Copyright (C) 2018-2026 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import json
from decimal import Decimal

import pytest
from cryptofeed.backends.backend import RetryPolicy
from cryptofeed.defines import L2_BOOK, TRADES

from cryptostore.config import ConfigError, WorkersConfig, cpu_count, feed_options, load, parse, resolve_exchange


def test_minimal_config_parses(base_config):
    config = parse(base_config)
    assert [exchange.name for exchange in config.exchanges] == ['COINBASE']
    coinbase = config.exchanges[0]
    assert coinbase.subscription == {TRADES: ['BTC-USD', 'ETH-USD'], L2_BOOK: ['BTC-USD']}
    # every channel routes to every backend unless told otherwise
    assert all(channel.backends == ('out',) for channel in coinbase.channels)
    assert config.backends['out'].type.name == 'tty'
    assert config.on_feed_error == 'raise'
    assert config.stats_interval == 60.0
    assert config.record is None
    assert config.log_level == 'INFO'


def test_json_file_loads(tmp_path, base_config):
    path = tmp_path / 'config.json'
    path.write_text(json.dumps(base_config))
    config = load(str(path))
    assert config.exchanges[0].name == 'COINBASE'


def test_yaml_file_loads(tmp_path):
    path = tmp_path / 'config.yaml'
    path.write_text('''
backends:
  out: {type: tty}
exchanges:
  coinbase:
    channels:
      trades: BTC-USD
''')
    config = load(str(path))
    assert config.exchanges[0].name == 'COINBASE'
    assert config.exchanges[0].channels[0].symbols == ('BTC-USD',)


def test_missing_file_is_a_config_error(tmp_path):
    with pytest.raises(ConfigError, match='not found'):
        load(str(tmp_path / 'nope.yaml'))


def test_env_var_is_resolved(tmp_path, monkeypatch):
    monkeypatch.setenv('CS_PORT', '5433')
    monkeypatch.setenv('CS_PW', 'hunter2')
    monkeypatch.setenv('CS_CONFIG_PATH', str(tmp_path / 'from_env.yaml'))
    (tmp_path / 'from_env.yaml').write_text('''
backends:
  pg:
    type: postgres
    host: db
    port: ${CS_PORT}
    database: ${CS_DB:-cryptostore}
    user: cryptostore
    password: pw-${CS_PW}
exchanges:
  COINBASE:
    channels: {trades: [BTC-USD]}
''')
    monkeypatch.setenv('CRYPTOSTORE_CONFIG', str(tmp_path / 'from_env.yaml'))
    config = load()
    options = config.backends['pg'].options
    assert options['port'] == 5433            # a whole-value reference gets its type back
    assert options['db'] == 'cryptostore'     # default applied, alias mapped to the constructor keyword
    assert options['pw'] == 'pw-hunter2'      # embedded reference stays a string


def test_unset_env_var_without_default_fails(base_config):
    base_config['backends']['out'] = {'type': 'socket', 'addr': '${NOT_SET_ANYWHERE}'}
    with pytest.raises(ConfigError, match='NOT_SET_ANYWHERE'):
        parse(base_config)


def test_unknown_exchange_suggests(base_config):
    base_config['exchanges']['COINBSE'] = base_config['exchanges'].pop('COINBASE')
    with pytest.raises(ConfigError, match="did you mean 'COINBASE'"):
        parse(base_config)


def test_exchange_name_is_case_insensitive():
    assert resolve_exchange('coinbase') == 'COINBASE'
    assert resolve_exchange('Binance_Futures') == 'BINANCE_FUTURES'


def test_unknown_channel_suggests(base_config):
    base_config['exchanges']['COINBASE']['channels'] = {'trade': ['BTC-USD']}
    with pytest.raises(ConfigError, match="did you mean 'trades'"):
        parse(base_config)


def test_channel_not_offered_by_exchange(base_config):
    base_config['exchanges']['COINBASE']['channels'] = {'funding': ['BTC-USD']}
    with pytest.raises(ConfigError, match='COINBASE does not provide funding'):
        parse(base_config)


def test_unknown_backend_reference(base_config):
    base_config['exchanges']['COINBASE']['backends'] = ['outt']
    with pytest.raises(ConfigError, match="'outt' is not a configured backend - did you mean 'out'"):
        parse(base_config)


def test_unknown_backend_type(base_config):
    base_config['backends']['out'] = {'type': 'postgre'}
    with pytest.raises(ConfigError, match="unknown backend type 'postgre' - did you mean 'postgres'"):
        parse(base_config)


def test_backend_type_required(base_config):
    base_config['backends']['out'] = {'host': 'x'}
    with pytest.raises(ConfigError, match='"type" is required'):
        parse(base_config)


def test_unknown_top_level_key(base_config):
    base_config['exchange'] = {}
    with pytest.raises(ConfigError, match="unknown key 'exchange' - did you mean 'exchanges'"):
        parse(base_config)


def test_per_channel_routing(base_config):
    base_config['backends']['stream'] = {'type': 'stream', 'tcp': ':0'}
    base_config['exchanges']['COINBASE']['backends'] = ['out']
    base_config['exchanges']['COINBASE']['channels']['l2_book'] = {'symbols': ['BTC-USD'], 'backends': ['stream', 'out']}
    config = parse(base_config)
    by_channel = {channel.channel: channel for channel in config.exchanges[0].channels}
    assert by_channel[TRADES].backends == ('out',)
    assert by_channel[L2_BOOK].backends == ('stream', 'out')
    assert config.backends_used() == ['out', 'stream']


def test_channel_with_no_backends_is_rejected(base_config):
    base_config['exchanges']['COINBASE']['channels']['trades'] = {'symbols': ['BTC-USD'], 'backends': []}
    with pytest.raises(ConfigError, match='no backends selected'):
        parse(base_config)


def test_symbols_must_be_a_list(base_config):
    base_config['exchanges']['COINBASE']['channels']['trades'] = {'symbols': []}
    with pytest.raises(ConfigError, match='expected a list of symbols'):
        parse(base_config)


def test_duplicate_symbols_collapse(base_config):
    base_config['exchanges']['COINBASE']['channels']['trades'] = ['BTC-USD', 'BTC-USD', 'ETH-USD']
    config = parse(base_config)
    assert config.exchanges[0].channels[0].symbols == ('BTC-USD', 'ETH-USD')


def test_backend_cannot_store_channel(base_config):
    base_config['backends']['pg'] = {'type': 'postgres', 'database': 'x', 'user': 'u'}
    base_config['exchanges']['DERIBIT'] = {'channels': {'l1_book': ['BTC-USD-PERP']}}
    with pytest.raises(ConfigError, match=r"backend 'pg' \(postgres\) cannot store l1_book"):
        parse(base_config)


def test_feed_options_are_validated(base_config):
    base_config['exchanges']['COINBASE']['options'] = {'max_dept': 10}
    with pytest.raises(ConfigError, match="does not take an option named 'max_dept' - did you mean 'max_depth'"):
        parse(base_config)


def test_reserved_feed_options_are_rejected(base_config):
    base_config['exchanges']['COINBASE']['options'] = {'symbols': ['BTC-USD']}
    with pytest.raises(ConfigError, match='set by cryptostore'):
        parse(base_config)


def test_exchange_specific_options_are_accepted(base_config):
    base_config['exchanges']['BINANCE'] = {'channels': {'l2_book': ['BTC-USDT']}, 'options': {'depth_interval': '1000ms', 'max_depth': 20, 'retries': -1}}
    config = parse(base_config)
    binance = next(exchange for exchange in config.exchanges if exchange.name == 'BINANCE')
    assert binance.options == {'depth_interval': '1000ms', 'max_depth': 20, 'retries': -1}


def test_feed_options_include_base_and_exchange_parameters():
    from cryptofeed.exchanges import Binance, Coinbase
    options = feed_options(Binance)
    assert {'depth_interval', 'max_depth', 'retries', 'candle_interval', 'sandbox', 'http_proxy'} <= options
    assert 'symbols' not in options and 'callbacks' not in options and 'config' not in options
    assert 'depth_interval' not in feed_options(Coinbase)


def test_backend_option_aliases_and_numeric_type(base_config):
    base_config['backends']['pg'] = {'type': 'postgres', 'host': 'db', 'database': 'cs', 'user': 'u', 'password': 'p', 'numeric_type': 'float'}
    config = parse(base_config)
    options = config.backends['pg'].options
    assert options == {'host': 'db', 'db': 'cs', 'user': 'u', 'pw': 'p', 'numeric_type': float}


def test_numeric_type_decimal(base_config):
    base_config['backends']['out'] = {'type': 'socket', 'addr': 'tcp://127.0.0.1', 'port': 1234, 'numeric_type': 'Decimal'}
    assert parse(base_config).backends['out'].options['numeric_type'] is Decimal


def test_bad_numeric_type(base_config):
    base_config['backends']['out'] = {'type': 'socket', 'addr': 'tcp://127.0.0.1', 'port': 1234, 'numeric_type': 'double'}
    with pytest.raises(ConfigError, match='numeric_type must be one of'):
        parse(base_config)


def test_required_backend_options(base_config):
    base_config['backends']['out'] = {'type': 'influxdb', 'host': 'http://x:8086', 'org': 'o'}
    with pytest.raises(ConfigError, match='influxdb backends require bucket, token'):
        parse(base_config)


def test_queue_options(base_config):
    base_config['backends']['out'] = {'type': 'socket', 'addr': 'tcp://127.0.0.1', 'port': 1,
                                   'queue': {'max_depth': 500, 'overflow': 'block', 'batch_interval': 0.1, 'retry': {'max_attempts': 3, 'base': 1}}}
    queue = parse(base_config).backends['out'].queue
    assert queue['max_depth'] == 500 and queue['overflow'] == 'block' and queue['batch_interval'] == 0.1
    assert queue['retry'] == RetryPolicy(max_attempts=3, base=1)


def test_bad_queue_options(base_config):
    base_config['backends']['out'] = {'type': 'socket', 'addr': 'tcp://127.0.0.1', 'port': 1, 'queue': {'overflow': 'drop'}}
    with pytest.raises(ConfigError, match='overflow must be'):
        parse(base_config)
    base_config['backends']['out']['queue'] = {'max_dept': 1}
    with pytest.raises(ConfigError, match="did you mean 'max_depth'"):
        parse(base_config)


def test_book_options(base_config):
    base_config['backends']['out'] = {'type': 'socket', 'addr': 'tcp://127.0.0.1', 'port': 1, 'book': {'snapshot_interval': 10, 'snapshots_only': True}}
    assert parse(base_config).backends['out'].book == {'snapshot_interval': 10, 'snapshots_only': True}
    base_config['backends']['out']['book'] = {'depth': 5}
    with pytest.raises(ConfigError, match="unknown key 'depth'"):
        parse(base_config)


def test_questdb_book_depth(base_config):
    base_config['backends']['out'] = {'type': 'questdb', 'host': 'q', 'book': {'depth': 5}}
    assert parse(base_config).backends['out'].book == {'depth': 5}


def test_stream_options_validated_at_parse_time(base_config):
    base_config['backends']['out'] = {'type': 'stream'}
    with pytest.raises(ConfigError, match='at least one of tcp, uds or zmq'):
        parse(base_config)
    base_config['backends']['out'] = {'type': 'stream', 'tcp': '0.0.0.0:99999'}
    with pytest.raises(ConfigError, match='out of range'):
        parse(base_config)
    base_config['backends']['out'] = {'type': 'stream', 'tcp': ':8080', 'tcp_port': 1}
    with pytest.raises(ConfigError, match='unknown stream option'):
        parse(base_config)


def test_tty_takes_no_options(base_config):
    base_config['backends']['out'] = {'type': 'tty', 'color': True}
    with pytest.raises(ConfigError, match='tty backends take no options'):
        parse(base_config)


def test_record_forms(base_config):
    base_config['record'] = '/data/captures'
    assert parse(base_config).record.path == '/data/captures'
    base_config['record'] = {'path': '/data/c.pcap', 'rotate_size': 1024, 'compress': False}
    record = parse(base_config).record
    assert (record.path, record.rotate_size, record.compress) == ('/data/c.pcap', 1024, False)
    base_config['record'] = {'rotate_size': 1024}
    with pytest.raises(ConfigError, match='"path"'):
        parse(base_config)


def test_log_and_misc_settings(base_config):
    base_config.update({'log': {'level': 'debug', 'file': '/var/log/cs.log'}, 'stats_interval': 0, 'health_file': '/tmp/h', 'on_feed_error': 'remove_feed',
                        'cryptofeed': {'uvloop': False, 'symbol_cache_ttl': 3600}})
    config = parse(base_config)
    assert (config.log_level, config.log_file, config.stats_interval, config.health_file, config.on_feed_error) == ('DEBUG', '/var/log/cs.log', 0.0, '/tmp/h', 'remove_feed')
    assert config.cryptofeed == {'uvloop': False, 'symbol_cache_ttl': 3600}


def test_cryptofeed_log_is_managed(base_config):
    base_config['cryptofeed'] = {'log': {'level': 'DEBUG'}}
    with pytest.raises(ConfigError, match='cryptofeed.log is managed'):
        parse(base_config)


def test_bad_misc_settings(base_config):
    for key, value, message in (('stats_interval', -1, 'stats_interval'), ('on_feed_error', 'ignore', 'on_feed_error'), ('log', {'level': 'LOUD'}, 'level must be')):
        bad = dict(base_config)
        bad[key] = value
        with pytest.raises(ConfigError, match=message):
            parse(bad)


def test_empty_sections(base_config):
    with pytest.raises(ConfigError, match='at least one backend'):
        parse({'exchanges': base_config['exchanges']})
    with pytest.raises(ConfigError, match='at least one exchange'):
        parse({'backends': base_config['backends']})
    with pytest.raises(ConfigError, match='empty'):
        parse(None)


def test_duplicate_exchange_entries(base_config):
    base_config['exchanges']['coinbase'] = {'channels': {'ticker': ['BTC-USD']}}
    with pytest.raises(ConfigError, match='configured twice'):
        parse(base_config)


def test_workers_forms(base_config):
    assert parse(base_config).workers == WorkersConfig(count=1, min_symbols=50, lag_warn=1.0)
    base_config['workers'] = 4
    assert parse(base_config).workers.count == 4
    base_config['workers'] = 'auto'
    assert parse(base_config).workers.count == cpu_count() >= 1
    base_config['workers'] = {'count': 2, 'min_symbols': 10, 'lag_warn': 0.5}
    workers = parse(base_config).workers
    assert (workers.count, workers.min_symbols, workers.lag_warn) == (2, 10, 0.5)
    for bad in (0, -1, 'many', True, {'count': 2, 'min_symbols': 0}, {'count': 2, 'lag_warn': 0}, {'threads': 2}):
        base_config['workers'] = bad
        with pytest.raises(ConfigError, match='workers'):
            parse(base_config)


def test_record_needs_one_worker(base_config, tmp_path):
    base_config['record'] = str(tmp_path / 'captures') + '/'
    base_config['workers'] = 2
    with pytest.raises(ConfigError, match='workers: 1'):
        parse(base_config)
    base_config['workers'] = 1
    assert parse(base_config).record is not None


def test_symbol_patterns_are_kept_for_expansion(base_config):
    base_config['exchanges']['COINBASE']['channels'] = {'trades': 'all', 'ticker': ['BTC-USD', '*-EUR'], 'l2_book': ['BTC-USD']}
    config = parse(base_config)
    trades, ticker, book = config.exchanges[0].channels
    assert trades.symbols == ('all',) and trades.patterns == ('all',)
    assert ticker.symbols == ('BTC-USD', '*-EUR') and ticker.patterns == ('*-EUR',)
    assert book.patterns == ()
    assert config.exchanges_with_patterns() == ['COINBASE'] and config.exchanges[0].patterns == ('all', '*-EUR')


def test_exchange_shards_option(base_config):
    assert parse(base_config).exchanges[0].shards is None
    base_config['exchanges']['COINBASE']['shards'] = 2
    assert parse(base_config).exchanges[0].shards == 2
    for bad in (0, True, 'two'):
        base_config['exchanges']['COINBASE']['shards'] = bad
        with pytest.raises(ConfigError, match='shards'):
            parse(base_config)
