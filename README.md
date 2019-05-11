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

### Requirements

Cryptostore currently requires either kafka or redis to be installed. The extra dependencies for your backend of choice must be installed as well (eg `pip install cryptostore[redis]`). Redis requires Redis Streams, which is supported in versions > 5.0.


### Running Cryptostore

Once installed with pip, an executable is placed on the path, so you can simply run `cryptostore` to start the collector. It requires a `config.yaml` file. If its not in the current working directory, you can specify the path to the config with the `--config` option.

An example [config](config.yaml), with documentation inline is provided in the root of the repository. The config file is monitored by cryptostore, so you can change the options in the file and it will apply them without the need to reload the service (this is experimental. If you encounter issues with it, please raise an issue).


### Running with other consumers

Cryptostore can operate with other consumers of the exchange data (eg. if a trading engine wants to consume the updates).

For Redis
  - Disable the message removal in the Redis settings in `config.yaml`. The other consumer will need to be responsible for
  message removal (if so desired), and it must ensure it doesnt remove messages before cryptostore has had a chance to process them.
  
For Kafka
  - You need only supply a different consumer group id for the other consumers to ensure all consumers receive all messages. Kafka's configuration controls the removal of committed messages in a topic (typically by time or size)


### Planned features
* [ ] Missing data detection and correction (for exchanges that support historical data, typically only trade data)
* [ ] Support other caching engines. Currently uses Redis Streams. Will also support kafka (and maybe others) in near future
* [ ] Storing data to InfluxDB
* [ ] Storing data to MongoDB
* [ ] Subscribing to Book Deltas
* [ ] Support for enabling computation and storage of diverse metrics in parallel with data collection (eg. configurable OHLCV)
* [ ] Support for forwarding data to another service/sink (eg. to a trading engine). 
