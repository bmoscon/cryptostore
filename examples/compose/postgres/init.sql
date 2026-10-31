-- Tables for cryptostore's postgres backend, one per data type (l1_book has no table layout).
-- Generated with cryptofeed's create_table_sql(); regenerate after upgrading cryptofeed:
--   python -c "from cryptofeed.backends import postgres as pg; print(pg.TradePostgres.create_table_sql())"

-- TradePostgres
CREATE TABLE IF NOT EXISTS trades (id serial PRIMARY KEY, "timestamp" TIMESTAMPTZ, "receipt_timestamp" TIMESTAMPTZ, "exchange" TEXT, "symbol" TEXT, "side" TEXT, "amount" NUMERIC, "price" NUMERIC, "trade_id" TEXT, "order_type" TEXT)

-- TickerPostgres
CREATE TABLE IF NOT EXISTS ticker (id serial PRIMARY KEY, "timestamp" TIMESTAMPTZ, "receipt_timestamp" TIMESTAMPTZ, "exchange" TEXT, "symbol" TEXT, "bid" NUMERIC, "ask" NUMERIC)

-- BookPostgres
CREATE TABLE IF NOT EXISTS book (id serial PRIMARY KEY, "timestamp" TIMESTAMPTZ, "receipt_timestamp" TIMESTAMPTZ, "exchange" TEXT, "symbol" TEXT, "data" JSONB)

-- FundingPostgres
CREATE TABLE IF NOT EXISTS funding (id serial PRIMARY KEY, "timestamp" TIMESTAMPTZ, "receipt_timestamp" TIMESTAMPTZ, "exchange" TEXT, "symbol" TEXT, "mark_price" NUMERIC, "rate" NUMERIC, "next_funding_time" TIMESTAMPTZ, "predicted_rate" NUMERIC)

-- OpenInterestPostgres
CREATE TABLE IF NOT EXISTS open_interest (id serial PRIMARY KEY, "timestamp" TIMESTAMPTZ, "receipt_timestamp" TIMESTAMPTZ, "exchange" TEXT, "symbol" TEXT, "open_interest" NUMERIC)

-- LiquidationsPostgres
CREATE TABLE IF NOT EXISTS liquidations (id serial PRIMARY KEY, "timestamp" TIMESTAMPTZ, "receipt_timestamp" TIMESTAMPTZ, "exchange" TEXT, "symbol" TEXT, "side" TEXT, "quantity" NUMERIC, "price" NUMERIC, "trade_id" TEXT, "status" TEXT)

-- CandlesPostgres
CREATE TABLE IF NOT EXISTS candles (id serial PRIMARY KEY, "timestamp" TIMESTAMPTZ, "receipt_timestamp" TIMESTAMPTZ, "exchange" TEXT, "symbol" TEXT, "candle_start" TIMESTAMPTZ, "candle_stop" TIMESTAMPTZ, "interval" TEXT, "trades" INTEGER, "open" NUMERIC, "close" NUMERIC, "high" NUMERIC, "low" NUMERIC, "volume" NUMERIC, "closed" BOOLEAN)

-- IndexPostgres
CREATE TABLE IF NOT EXISTS index (id serial PRIMARY KEY, "timestamp" TIMESTAMPTZ, "receipt_timestamp" TIMESTAMPTZ, "exchange" TEXT, "symbol" TEXT, "price" NUMERIC)

