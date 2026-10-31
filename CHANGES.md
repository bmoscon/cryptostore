## Changelog

### 2.0.0 (2026-10-31)
  * Update: Rewritten for cryptofeed 3.x, Python 3.15
  * Update: Configuration moved from environment variables to one YAML/JSON file. 
  * Feature: `stream` backend - a built-in server that binds TCP, unix socket and ZeroMQ PUB endpoints and fans every update out to connected applications. Slow TCP clients are disconnected instead of silently skipped
  * Feature: Every cryptofeed backend: PostgreSQL, Redis (ZSET and Streams), MongoDB, InfluxDB 2, QuestDB, Kafka, client-mode socket and ZeroMQ, plus a tty debug backend. One writer per backend and channel, shared across exchanges
  * Feature: `l1_book` and `index` channels on every backend that takes JSON documents
  * Feature: `workers` - spread the exchanges over worker threads, each with its own event loop and FeedHandler. Large subscriptions are sharded across them (`shards` caps an exchange). Parallel on free-threaded Python
  * Feature: event-loop lag metric per worker and for the main loop in the stats report and health file, with a warning when a worker falls behind
  * Feature: `symbols: all` and glob patterns such as `*-USDT`, expanded from the exchange's symbol list at start-up
  * Feature: `record` - raw exchange traffic capture to pcap via cryptofeed 
  * Feature: `cryptostore` command line: `run`, `check` (with `--symbols`), `exchanges`, `backends`, `health`
  * Feature: Periodic stats reports, health file and a container `HEALTHCHECK`
  * Feature: GitHub Actions for lint and tests (3.15, 3.15t); publishing a GitHub release pushes the multi-arch images to ghcr.io and the package to PyPI
  * Update: Multi-stage Dockerfile on debian slim with a uv-managed interpreter, running as an unprivileged user

### 1.0.3 (2023-01-28)
  * Feature: Add tool to generate docker run commands
  * Bugfix: Fixed a kwarg typo

### 1.0.2 (2022-03-01)
  * Feature: QuestDB support

### 1.0.1 (2022-02-26)
  * Feature: Add support for socket backends (TCP, UDS, UDP)
  * Example: Add example for receiving TCP messages from cryptofeed via container
  * Feature: Support for InfluxDB

### 1.0.0 (2022-02-18)
  * Update: Old repo moved to legacy branch, new version (1.X) created.
  * Feature: Support for public channels on Redis and Mongo
  * Bugfix: Input validation, remove unused callbacks from Feed.
  * Docs: Start documentation.
  * Feature: Add support for Redis Streams
  * Feature: Add support for Postgres
