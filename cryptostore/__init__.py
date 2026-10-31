'''
Copyright (C) 2018-2026 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from importlib.metadata import PackageNotFoundError, version


try:
    __version__ = version('cryptostore')
except PackageNotFoundError:  # running from a checkout that was never installed
    __version__ = '0.0.0'

__all__ = ['__version__']
