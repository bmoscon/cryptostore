# Cryptostore

[![License](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/Python-3.15%20%7C%203.15t-green.svg)
[![Container](https://img.shields.io/badge/ghcr.io-bmoscon%2Fcryptostore-brightgreen.svg)](https://github.com/bmoscon/cryptostore/pkgs/container/cryptostore)
[![PyPI](https://img.shields.io/badge/PyPI-cryptostore-brightgreen.svg)](https://pypi.org/project/cryptostore/)

Cryptostore subscribes to cryptocurrency exchanges with [cryptofeed](https://github.com/bmoscon/cryptofeed),
stores the normalized data in your database and streams it in real time to your own applications. One config file 
powers the whole collection. You can run cryptostore as a container, or as a Python package.

```yaml
backends:
  postgres:
    type: postgres
    host: db
    database: cryptostore
    user: cryptostore
    password: ${PGPASSWORD}
  stream:
    type: stream            # cryptostore listens, your programs connect
    tcp: 0.0.0.0:8080
    zmq: tcp://0.0.0.0:5555

exchanges:
  COINBASE:
    channels:
      trades: [BTC-USD, ETH-USD]
      l2_book: {symbols: [BTC-USD], backends: [stream]}    # books only to the stream
    options:
      max_depth: 25
  BINANCE_FUTURES:
    channels:
      trades: all                        # every symbol the exchange lists
      liquidations: ['*-USDT-PERP']      # or a pattern
      funding: [BTC-USDT-PERP]

workers: 4                               # threads
```

* **Exchanges and channels**: everything cryptofeed supports - 30 exchanges, `trades`, `l2_book`, `l3_book`, `l1_book`, `ticker`, `funding`, `open_interest`, `liquidations`, `index`, `candles`
* **Storage backends**: PostgreSQL, Redis (sorted sets or streams), MongoDB, InfluxDB 2, QuestDB, Kafka
* **Streaming**: a built-in server that fans every update out over TCP, unix sockets and ZeroMQ PUB, plus cryptofeed's client-mode socket and ZeroMQ outputs
* **Routing**: per exchange or per channel, any number of backends at once
* **Symbols**: explicit lists, `all`, or glob patterns expanded from the exchange's symbol list
* **Scaling**: large subscriptions are sharded across worker threads, each with its own event loop. On free-threaded Python (3.15t) they run on separate cores
* **Raw capture**: optionally record every exchange message to pcap files cryptofeed can replay
* **Operations**: strict config validation with suggestions, stats reports with an event-loop lag metric, a health file for container health checks, graceful shutdown on `SIGTERM`

## Running with Docker

```sh
docker pull ghcr.io/bmoscon/cryptostore:latest
docker run --rm -v "$PWD/config.yaml:/config/config.yaml:ro" -p 8080:8080 ghcr.io/bmoscon/cryptostore:latest
```

The container reads `/config/config.yaml` (override with `CRYPTOSTORE_CONFIG` or pass the path after
`run`), writes to `/data`, and runs as an unprivileged user. The image runs CPython 3.15t, the
free-threaded build, so `workers` run in parallel out of the box. `latest`, `2.0.0` and `2.0` all
name that same image; there is no GIL variant on ghcr.io.

Build locally with `docker build -t cryptostore .`. A GIL build is still possible for local use with
`docker build --build-arg PYTHON_VERSION=3.15 -t cryptostore:gil .`.

[examples/compose](examples/compose) has ready to run docker compose set-ups: Postgres plus streaming,
and streaming only.

## Running without Docker

Python 3.15 or 3.15t is required.

```sh
uv tool install 'cryptostore[all]'      # or: pip install 'cryptostore[all]'
cryptostore run config.yaml
```

The `all` extra installs every backend's driver. Pick individual ones with `cryptostore[postgres]`,
`[redis]`, `[mongo]`, `[kafka]`, `[zmq]` (the stream backend's TCP and unix endpoints need nothing extra).

**Free-threaded Python.** Every dependency keeps the GIL disabled on 3.15t. 
cryptostore logs a warning at start-up when it finds the GIL enabled on a free-threaded build, and the health file reports `gil_enabled`.

## Commands

| command | purpose |
| --- | --- |
| `cryptostore run [config]` | run the collector |
| `cryptostore check [config]` | validate a config and print the worker plan without connecting to any backend or feed. Symbol patterns are expanded, which downloads those exchanges' symbol lists. `--symbols` also verifies every explicit symbol exists |
| `cryptostore exchanges [name]` | list exchanges and their channels, or one exchange with the options it accepts |
| `cryptostore backends` | list backend types and the channels each can store |
| `cryptostore health [config]` | exit 0 if the running collector's health file is fresh - the container `HEALTHCHECK` |

## Configuration

YAML or JSON is accepted. Cryptostore looks at the path
on the command line, `$CRYPTOSTORE_CONFIG`, or `./config.yaml`. The container sets
`CRYPTOSTORE_CONFIG=/config/config.yaml`, so mounting a file there is enough.
[examples/config.yaml](examples/config.yaml) shows every option with its default.

Any string value may contain `${NAME}` or `${NAME:-default}`. A reference to an unset variable with no
default is an error. When the whole value is one reference the result is parsed as YAML, so
`port: ${PGPORT}` becomes an integer.

Validation is strict and runs before anything connects: unknown keys, exchanges, channels, backends and
feed options will cause errors.

### `backends`

A mapping of backend name to definition. They map to crytofeed backends except these three:

| key | purpose |
| --- | --- |
| `book` | extra arguments for the `l2_book`/`l3_book` writer: `snapshot_interval` (full book every N deltas, default 1000) and `snapshots_only` (never send deltas). The questdb backend takes `depth` instead |
| `queue` | cryptofeed `BackendQueue` tuning: `max_depth`, `overflow` (`drop_oldest`, `drop_new`, `block`), `batch_max`, `batch_interval`, `flush_deadline`, `retry: {max_attempts, base, jitter, max_delay}` |
| `numeric_type` | `decimal`, `float` or `str` - how prices and sizes are written. Each type has cryptofeed's default when omitted |

| type | what it does | options (aliases in parentheses) |
| --- | --- | --- |
| `stream` | cryptostore's streaming server. Binds endpoints that your applications connect to. See [Streaming](#streaming) | `tcp` (`host:port`), `uds` (socket path), `zmq` (PUB endpoint), `client_queue`, `zmq_hwm` |
| `postgres` | PostgreSQL, one table per data type. Tables must exist, see [PostgreSQL tables](#postgresql-tables) | `host`, `port`, `db` (`database`), `user`, `pw` (`password`), `table`, `custom_columns` |
| `redis` | Redis sorted sets, one per exchange and symbol, scored by timestamp | `host`, `port`, `socket`, `key`, `score_key` |
| `redis_stream` | Redis Streams (`XADD`), one stream per exchange and symbol | `host`, `port`, `socket`, `key` |
| `mongo` | MongoDB, one collection per data type | `db` (`database`), `host`, `port`, `uri`, `key`, `client_options` |
| `influxdb` | InfluxDB 2.x write API | `addr` (`host`, `url`), `org`, `bucket`, `token`, `key` |
| `questdb` | QuestDB line protocol over HTTP. Books are stored as the top `depth` levels | `host`, `port`, `key` |
| `kafka` | Kafka. Every option other than `key`/`numeric_type`/`none_to` goes to `AIOKafkaProducer` | `bootstrap_servers`, `client_id`, `acks`, ... |
| `socket` | cryptofeed's client-mode socket backend: connects to a listener you run | `addr` (`host`, `url`) as `tcp://`, `udp://` or `uds://`, `port`, `mtu` |
| `zmq` | cryptofeed's client-mode ZeroMQ PUB: connects to a SUB you bind | `host`, `port`, `key`, `dynamic_key` |
| `tty` | prints every update to stdout, for debugging | none |

Every type takes every channel except Postgres, which has no table layout for `l1_book`. Each worker
creates one writer per backend and channel, shared by every exchange that worker handles, so twenty
exchanges writing trades to one Postgres backend share one connection pool per worker.

### `exchanges`

A mapping of cryptofeed exchange id (case-insensitive) to what to subscribe to. `cryptostore exchanges`
lists them. `cryptostore exchanges binance` shows one with the options it accepts.

```yaml
exchanges:
  COINBASE:
    backends: [postgres, stream]   # default for this exchange's channels (default: every backend)
    channels:
      trades: [BTC-USD, ETH-USD]   # a list of symbols routes to the exchange's default backends
      l2_book:                     # or a mapping to route one channel elsewhere
        symbols: all
        backends: [stream]
    options:                       # keyword arguments for the cryptofeed Feed
      max_depth: 25
      retries: -1
    shards: 4                      # at most this many shards when split across workers (default: no limit)
```

Symbols follow cryptofeed's normalized naming (`BTC-USD`, `BTC-USDT-PERP`, `BTC-USD-26H05`). A symbol
list may also contain `all` or glob patterns (`*-USDT`, `BTC-*`, `*-USDT-PERP`), which are expanded from
the exchange's symbol list when cryptostore starts. A pattern that matches nothing is an error. Explicit
symbols are checked when the feed starts, or ahead of time with `cryptostore check --symbols`.

### `workers`

```yaml
workers:
  count: 4            # worker threads, or auto for one per CPU. Default 1
  min_symbols: 50     # split an exchange only into shards of at least this many symbols. Default 50
  lag_warn: 1.0       # seconds of event-loop lag that log a warning. Default 1
```

`workers: 4` is shorthand for `count: 4`. Each worker is a thread with its own event loop running its
own cryptofeed `FeedHandler`. An exchange with enough symbols is split into shards (every n-th symbol
of every channel), shards are dealt to workers largest first, and workers that would get nothing are
dropped. `cryptostore check` prints the plan. See [Scaling](#scaling) for why and when.

`record` needs `count: 1`: cryptofeed's recorder is bound to one event loop.

### Everything else

| key | default | purpose |
| --- | --- | --- |
| `record` | off | record the raw exchange traffic to pcap files cryptofeed can replay. A path string, or `{path, rotate_size, compress}`. A directory gets timestamped files |
| `log` | `{level: INFO}` | `level` and an optional rotating `file` (stdout is always used) |
| `stats_interval` | `60` | seconds between stats reports. `0` disables them and the health file |
| `health_file` | off | a JSON file rewritten with every stats report, see [Stats and health](#stats-and-health). `cryptostore health` exits 0 while it is fresh, which is what the container `HEALTHCHECK` runs |
| `on_feed_error` | `raise` | `raise`: a permanently failed feed stops cryptostore (exit non-zero, let the container restart). `remove_feed`: drop it and keep the rest running |
| `cryptofeed` | `{}` | passed to cryptofeed's `Config`: `uvloop`, `symbol_cache_ttl`, `ignore_invalid_instruments`, and per-exchange credentials. `log` is managed by cryptostore and not allowed here |

## Streaming

The `stream` backend turns cryptostore into a market data server: cryptostore binds the endpoints and
your applications connect. Every update routed to the backend goes to every connected client, filter on
the client side, or with a ZeroMQ subscription prefix.

| endpoint | option | wire format |
| --- | --- | --- |
| TCP | `tcp: 0.0.0.0:8080` | newline-delimited JSON frames `{"type": "<channel>", "data": {...}}` |
| Unix socket | `uds: /data/cryptostore.sock` | the same frames |
| ZeroMQ PUB | `zmq: tcp://0.0.0.0:5555` | `<EXCHANGE>-<channel>-<SYMBOL> <json>`, so a SUB socket can subscribe by prefix |

`data` is the update as cryptofeed seriaizes it plus `receipt_timestamp`. Prices and sizes are exact
decimals on the wire (`numeric_type: decimal`, the default), so parse them as decimals. A book frame
carries either `book` (a full snapshot: `{"bid": {price: size}, "ask": {...}}`) or `delta` (changed
levels as `[price, size]` pairs, size 0 means removed). A full book is sent first, then deltas, then
another full book every `snapshot_interval` deltas so late joiners can resynchronize. `snapshots_only`
sends only full books. The TCP and unix frames are byte for byte what cryptofeed's own socket backend
writes, and the ZeroMQ messages match cryptofeed's ZeroMQ backend, so readers written for either work.

Each TCP and unix client has its own queue of `client_queue` frames (default 10000). A client that falls
further behind is disconnected rather than silently fed a gap: a client building a book from deltas needs
to know it missed data, and reconnecting gets it a fresh snapshot. ZeroMQ applies its own high-water
mark (`zmq_hwm`) and drops for slow subscribers, as PUB sockets do.

With several workers, frames from different shards interleave, but a symbol lives in exactly one shard
so every client sees each symbol's updates in order.

[examples/clients/tcp_client.py](examples/clients/tcp_client.py) and
[examples/clients/zmq_client.py](examples/clients/zmq_client.py) are complete readers. From another
language, read lines from the socket and parse each as JSON.

## Scaling

cryptostore is one process. Without `workers` everything runs on one event loop, and one loop is one
core: parsing, order book maintenance and serialisation for every subscription share it. That is plenty
for a few hundred symbols. It is not enough for every book on a large exchange.

When a loop cannot keep up, nothing is dropped at first. Each websocket connection buffers up to 1024
frames, then stops reading, so TCP backpressure builds and latency grows. After about a minute the
keepalive timeout closes the connection, cryptofeed reconnects, and for book channels every symbol's
snapshot is fetched again under the exchange's REST rate limits. The stats report shows this as
climbing `loop_lag`, then rising `reconnects`.

`workers` is the fix. Set `count` to the cores you can spend and run on free-threaded Python, which is
what the container image ships: the worker threads then run in parallel, each with its own share of the
shards, while the main loop only fans frames out to stream clients and writes the stats. On the standard
3.15 interpreter the same config still runs correctly, but the threads take turns on one core, so there
prefer several processes with the config split between them. Each worker has its own database writers,
so a Postgres backend with four workers holds four connection pools.

Every exchange still limits connections and REST weight per IP. `shards` caps how many connections one
exchange is split into, cryptofeed's `delay_start` option staggers reconnects, and splitting across
containers on different hosts splits the IP limits too.

## Stats and health

Every `stats_interval` seconds cryptostore logs, and writes to `health_file` as JSON:

| field | content |
| --- | --- |
| `timestamp`, `version`, `python`, `free_threaded`, `gil_enabled` | when, and on what |
| `feeds`, `removed_feeds` | running feeds by shard label (`BINANCE[2/4]`), and feeds dropped by `on_feed_error: remove_feed` |
| `workers` | per worker: `feeds`, `symbols`, `alive`, `loop_lag` (seconds the loop is currently behind) and `loop_lag_max` (worst since the previous report) |
| `main_loop` | the same lag fields for the main loop (stream fan-out) |
| `backends` | per backend and channel, summed over the workers' writers: `written`, `qsize`, `batches`, `dropped_overflow`, `dropped_failed`, `retries`, `last_error`, `last_write_ts`. Stream backends add `clients`, `connections`, `published`, `kicked` |
| `connections` | per exchange websocket: `received`, `reconnects`, `watchdog_trips`, `open` |

A worker whose `loop_lag_max` exceeds `workers.lag_warn` logs a warning. `cryptostore health` checks
that the file is fresh and lists feeds. `docker ps` shows the result.

## PostgreSQL tables

The postgres backend inserts into tables that must already exist, one per data type.
[examples/compose/postgres/init.sql](examples/compose/postgres/init.sql) creates all of them and is what
the compose example mounts into Postgres. cryptofeed's classes print their own `CREATE TABLE`:

```sh
python -c "from cryptofeed.backends import postgres as pg; print(pg.TradePostgres.create_table_sql())"
```

`table` renames the table a backend writes to, `custom_columns` maps update fields to your own columns.

## Development

```sh
uv sync --all-extras
uv run ruff check .
uv run pytest
make test-ft        # the same tests on the free-threaded interpreter
```

The tests need no network and no databases. CI runs lint and the tests on 3.15 and 3.15t, builds the
free-threaded container on amd64 and arm64, and publishes the release image and the PyPI package when a
GitHub release is published.

## Releasing

Bump `version` in `pyproject.toml`, add the entry to `CHANGES.md`, commit, then create a GitHub release
whose tag is `v<version>` (the release page can create the tag). Publishing the release triggers two
workflows:

* `pypi.yml` builds the wheel and sdist, checks that the version matches the tag, and uploads to PyPI
  through trusted publishing - no API token is stored.
* `docker.yml` builds the free-threaded (3.15t) image for amd64 and arm64 and pushes it as `<version>`,
  `<major.minor>` and `latest` to `ghcr.io/bmoscon/cryptostore`. The GIL interpreter is only
  distributed through PyPI.

One-time setup, both in the browser: on pypi.org open the `cryptostore` project, *Publishing*, and add a
GitHub publisher with owner `bmoscon`, repository `cryptostore`, workflow `pypi.yml`, environment
`pypi`, then create an environment named `pypi` under the repository's *Settings, Environments*. On
GitHub open the `cryptostore` package under *Packages*, *Package settings*, *Manage Actions access*, and
give this repository the write role - needed because the package predates these workflows.


## Future Work

More features are in the works! If you'd like to discuss ongoing development, please join the [discord](https://discord.gg/zaBYaGAYfR).

## Contributing

Issues and PRs are welcomed!

For questions about the data itself (symbols, channels, exchanges, etc) see [cryptofeed](https://github.com/bmoscon/cryptofeed).

## Donations / Support

Support and donations are appreciated but not required. You can donate via [GitHub Sponsors](https://github.com/sponsors/bmoscon), or via the addresses below:

* Bitcoin: bc1qm0kxz8hqacaglku5fjhfe9a5hjnuyfwk02lsyr
* Ethereum: 0x690709FEe13eEce9E7852089BB2D53Ae5D073154
