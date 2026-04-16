-- ClickHouse initialization for Real-Time Crypto Market Monitoring Pipeline
-- Database: stocks

CREATE DATABASE IF NOT EXISTS stocks;

CREATE TABLE IF NOT EXISTS stocks.crypto_ticks_raw
(
    event_time DateTime64(3),
    symbol LowCardinality(String),
    product_id LowCardinality(String),
    price Float64,
    volume Float64,
    source LowCardinality(String),
    event_type LowCardinality(String),
    ingest_time DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (symbol, event_time);

CREATE TABLE IF NOT EXISTS stocks.crypto_metrics_1m
(
    symbol LowCardinality(String),
    product_id LowCardinality(String),
    window_start DateTime,
    window_end DateTime,
    open_price Float64,
    close_price Float64,
    avg_price Float64,
    min_price Float64,
    max_price Float64,
    total_volume Float64,
    avg_trade_size Float64,
    trade_count UInt64,
    price_stddev Float64,
    return_pct_1m Float64,
    price_range_pct Float64,
    volatility_pct Float64,
    is_price_spike UInt8,
    is_volume_anomaly UInt8,
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (symbol, window_start);

-- Helpful views/queries you can use later
-- Latest metrics per symbol:
-- SELECT *
-- FROM stocks.crypto_metrics_1m
-- ORDER BY window_start DESC
-- LIMIT 20;

-- Latest raw ticks per symbol:
-- SELECT *
-- FROM stocks.crypto_ticks_raw
-- ORDER BY event_time DESC
-- LIMIT 20;
