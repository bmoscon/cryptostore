# Cryptostore

[![License](https://img.shields.io/badge/license-XFree86-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/Python-3.6+-green.svg)
[![PyPi](https://img.shields.io/badge/PyPi-cryptostore-brightgreen.svg)](https://pypi.python.org/pypi/cryptostore)



A storage engine for cryptocurrency market data. You supply the exchanges, data type (trade, book, etc), and trading pairs you're interested in and Cryptostore does the rest!

Stores data to:
* Parquet
* Arctic
* Google Cloud Storage
* Amazon S3


### Running Cryptostore

Once installed with pip, an executable is placed on the path, so you can simply run `cryptostore` to start the collector. It requires a `config.yaml` file. If its not in the current working directory, you can specify the path to the config with the `--config` option.

An example [config](config.yaml), with documentation inline is provided in the root of the repository. The config file is monitored by cryptostore, so you can change the options in the file and it will apply them without the need to reload the service (this is experimental. If you encounter issues with it, please raise an issue).


### Planned features
* [ ] Missing data detection and correction (for exchanges that support historical data, typically only trade data)
* [ ] Support other caching engines. Currently uses Redis Streams. Will also support kafka (and maybe others) in near future
