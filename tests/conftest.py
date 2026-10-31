'''
Copyright (C) 2018-2026 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import copy
import sys
import sysconfig

import pytest
from cryptofeed.symbols import Symbols


BASE_CONFIG = {
    'backends': {
        'out': {'type': 'tty'},
    },
    'exchanges': {
        'COINBASE': {
            'channels': {
                'trades': ['BTC-USD', 'ETH-USD'],
                'l2_book': ['BTC-USD'],
            },
        },
    },
}


@pytest.fixture
def base_config():
    return copy.deepcopy(BASE_CONFIG)


@pytest.fixture(autouse=True)
def clear_symbols():
    Symbols.clear()
    yield
    Symbols.clear()


def pytest_sessionfinish(session, exitstatus):
    if sysconfig.get_config_var('Py_GIL_DISABLED') and sys._is_gil_enabled():
        reporter = session.config.pluginmanager.get_plugin('terminalreporter')
        if reporter:
            reporter.write_line('the GIL was enabled during a free-threaded run - an imported package does not declare free-threading support', red=True)
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
