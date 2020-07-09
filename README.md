# Cryptostore

[![License](https://img.shields.io/badge/license-XFree86-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/Python-3.7+-green.svg)
[![PyPi](https://img.shields.io/badge/PyPi-cryptostore-brightgreen.svg)](https://pypi.python.org/pypi/cryptostore)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/da2a982c976649e193c807895ee7a33c)](https://www.codacy.com/manual/bmoscon/cryptostore?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=bmoscon/cryptostore&amp;utm_campaign=Badge_Grade)

A storage engine for cryptocurrency market data. You supply the exchanges, data type (trade, book, etc), and trading pairs you're interested in and Cryptostore does the rest!


### Requirements

Cryptostore currently requires either Kafka or Redis to be installed. The extra dependencies for your backend of choice must be installed as well (eg `pip install cryptostore[redis]`). Redis requires Redis Streams, which is supported in versions >= 5.0.


### Running Cryptostore

Once installed with pip, an executable is placed on the path, so you can simply run `cryptostore` to start the collector. It requires a `config.yaml` file. If its not in the current working directory, you can specify the path to the config with the `--config` option.

An example [config](config.yaml), with documentation inline is provided in the root of the repository. The config file is monitored by cryptostore, so you can change the options in the file and it will apply them without the need to reload the service (this is experimental. If you encounter issues with it, please raise an issue).


### Storing data

Stores data to:
* [Arctic](https://github.com/manahl/arctic)
* [InfluxDB](https://github.com/influxdata/influxdb)
* Elasticsearch
* Parquet, either on your local drive, or in the cloud using:
  * Google Cloud Storage
  * Amazon S3
  * [Google Drive](/docs/google_drive.md)


### Backfilling Trade Data

Cryptstore can backfill trade data - but be aware not all exchanges support historical trade data, and some only provide a limited amount. Backfill fills from the earliest date in data storage until the start date specified in the config. Backfill is restartable.


### Running with other consumers

Cryptostore can operate with other consumers of the exchange data (eg. a trading engine consuming updates).

For Redis
  - Disable the message removal in the Redis settings in `config.yaml`. The other consumer will need to be responsible for
  message removal (if so desired), and it must ensure messages are not removed before cryptostore has had a chance to process them.
  
For Kafka
  - You need only supply a different consumer group id for the other consumers to ensure all consumers receive all messages. Kafka's configuration controls the removal of committed messages in a topic (typically by time or size).

With a pass through
  - Cryptostore supports forwarding realtime data using ZeroMQ. To enable, use the `pass_through` option in the config. Data will be sent in real time (not subject to aggregation in redis/kafka). This can be used with or without data aggregation and storage.  


### Running in a container

You can run Cryptostore in a docker container. A Dockerfile and a docker-compose.yml are provided. It uses the config in config-docker.yaml, and its set up to use redis and store the data into Arctic/MongoDB. The port is mapped to 37017 (as opposed to 27017) so when connecting to Arctic from outside the container make sure you specify the port. Additionally, a volume should be configured in the docker-compose so that the mongoDB data will persist across restarts.


### Planned features

* [ ] More data types (eg. open interest)
* [ ] Postgres support
* [ ] Missing data detection and correction (for exchanges that support historical data, typically only trade data)
* [ ] Storing data to MongoDB
* [ ] Support for enabling computation and storage of diverse metrics in parallel with data collection (eg. configurable OHLCV)


## Contributing
Issues and PRs are welcomed. If you'd like to discuss ongoing development please join the [slack](https://join.slack.com/t/cryptofeed-dev/shared_invite/enQtNjY4ODIwODA1MzQ3LTIzMzY3Y2YxMGVhNmQ4YzFhYTc3ODU1MjQ5MDdmY2QyZjdhMGU5ZDFhZDlmMmYzOTUzOTdkYTZiOGUwNGIzYTk)
 (use the #cryptostore channel).
