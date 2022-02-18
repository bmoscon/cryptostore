## High Level Overview


Cryptostore utilizes the supports backends of cryptofeed to store data from exchanges in various databases and other services. The following are supported data stores / destinations allong with the flag that should be used in the BACKEND environment variable:

* Redis Sorted Sets (ZSET) - `REDIS`
* Redis Streams - `REDISSTREAM`
* MongoDB - `MONGO`

Cryptostore runs in a docker container, and expects configuration to be provided to it via environment variables. The env vars it expects (not all are requires for all configurations) are:

* EXCHANGE - the exchange. Only one can be specified. Run multiple containers to collect data from more than 1 exchange. Should be in all caps.
* SYMBOLS - a single symbol, or a list of symbols. Must follow cryptofeed naming conventions for symbols.
* CHANNELS - cryptofeed data channels (trades, l2book, ticker, etc.) Can be a single channel or a list of channels.
* CONFIG - path to cryptofeed config file (must be built into the container or otherwise accessible in the container). Not required. 
* BACKEND - Backend to be used, should be in all caps. 
* SNAPSHOT_ONLY - Valid for book data only, specifies that only complete books should be stored. Default is False.
* SNAPSHOT_INTERVAL - Specifies how often snapshot is stored in terms of number of delta updates between snapshots. Default is 1000.
* HOST - Host for backend. Defaults to localhost.
* PORT - Port for service. Defaults to backend default.
* CANDLE_INTERVAL - Used for candles. Default is 1m.
* DATABASE - Specify the database for MongoDB.


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
