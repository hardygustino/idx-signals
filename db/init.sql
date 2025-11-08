CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS raw_prices (
    symbol TEXT NOT NULL,
    ts_utc TIMESTAMPTZ NOT NULL,
    open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, volume BIGINT,
    load_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, ts_utc)
);
CREATE INDEX IF NOT EXISTS idx_raw_prices_symbol_ts ON raw_prices(symbol, ts_utc DESC);


CREATE TABLE IF NOT EXISTS stg_prices_1h (
    symbol TEXT NOT NULL,
    ts_utc TIMESTAMPTZ NOT NULL,
    open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, volume BIGINT,
    PRIMARY KEY (symbol, ts_utc)
);


CREATE TABLE IF NOT EXISTS dwh_features_prices (
    symbol TEXT NOT NULL,
    ts_utc TIMESTAMPTZ NOT NULL,
    ma5 NUMERIC, ma20 NUMERIC, rsi14 NUMERIC, vol_z NUMERIC,
    PRIMARY KEY (symbol, ts_utc)
);


CREATE TABLE IF NOT EXISTS signals (
    signal_id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    ts_utc TIMESTAMPTZ NOT NULL,
    signal_type TEXT NOT NULL,
    score NUMERIC,
    details JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_signals_symbol_ts ON signals(symbol, ts_utc DESC);