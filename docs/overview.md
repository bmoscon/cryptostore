## High Level Overview


Cryptostore utilizes the supported backends of cryptofeed to store data from exchanges in various databases and other services. The following are the supported backends along with the flag that should be used in the BACKEND environment variable:

* Redis Sorted Sets (ZSET) - `REDIS`
* Redis Streams - `REDISSTREAM`
* MongoDB - `MONGO`
* Postgres - `POSTGRES`
* TCP - `TCP`
* UDP - `UDP`
* UDS - `UDS`
* InfluxDB - `INFLUX`
* QuestDB - `QUEST`

Cryptostore runs in a docker container, and expects configuration to be provided to it via environment variables. The env vars it utilizes (not all are required for all configurations) are:

* `EXCHANGE` - the exchange. Only one can be specified. Run multiple containers to collect data from more than 1 exchange. Should be in all caps.
* `SYMBOLS` - a single symbol, or a list of symbols. Must follow cryptofeed naming conventions for symbols.
* `CHANNELS` - cryptofeed data channels (`trades`, `l2_book`, `ticker`, etc.) Can be a single channel or a list of channels.
* `CONFIG` - path to cryptofeed config file (must be built into the container or otherwise accessible in the container). Not required. 
* `BACKEND` - Backend to be used, see list of supported backends above.
* `SAVE_RAW` - Keep raw data and save it to file. Default is False, and even when True you still have to specify a backend for the processed data. To persist the raw data files you should bind the /raw_data folder in the container to a local volume. E.g. add `-v /your/local/path:/raw_data` to your `docker run` command.
* `SNAPSHOT_ONLY` - Valid for orderbook channel only, specifies that only complete books should be stored. Default is False.
* `SNAPSHOT_INTERVAL` - Specifies how often snapshot is stored in terms of number of delta updates between snapshots. Default is 1000.
* `HOST` - Host for backend. Defaults to `localhost`. TCP, UDP and UDS require the protocol to be prepended to the host/url. E.g. `tcp://127.0.0.1`, `uds://udsfile.tmp`, etc.
* `PORT` - Port for service. Defaults to backend default.
* `CANDLE_INTERVAL` - Used for candles. Default is 1m.
* `DATABASE` - Specify the database for MongoDB or Postgres
* `USER` - the username for Postgres
* `PASSWORD` - the password for the specified Postgres user
* `ORG` - the InfluxDB organization
* `BUCKET` - the InfluxDB bucket
* `TOKEN` - the InfluxDB token


### Example

An example of how the configuration would look when subscribing to BTC-USD and ETH-USD on Coinbase for Trades, Ticker and L2 Book channels, using Redis Streams:

```
docker run -e EXCHANGE='COINBASE' \
           -e CHANNELS='trades,ticker,l2_book' \
           -e SYMBOLS='BTC-USD,ETH-USD' \
           -e BACKEND='REDISSTREAM' \
           ... networking and other params ... \
           cryptostore:latest
```
