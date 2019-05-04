# Cryptostore

[![License](https://img.shields.io/badge/license-XFree86-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/Python-3.6+-green.svg)
[![PyPi](https://img.shields.io/badge/PyPi-cryptostore-brightgreen.svg)](https://pypi.python.org/pypi/cryptostore)



A storage engine for crypto market data. You supply the exchanges, data type (trade, book, etc), and trading pairs you're interested in and Cryptostore does the rest!

Stores data to:
* Parquet
* Arctic
* Google Cloud Storage
* Amazon S3

Support a dynamic configuration, removing the requirement that the service be restarted to pick up configuration changes.

### Planned features
* [ ] Missing data detection and correction (for exchanges that support historical data, typically only trade data)
