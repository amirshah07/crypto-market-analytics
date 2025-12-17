CREATE TABLE IF NOT EXISTS coins (
    coin_key SERIAL PRIMARY KEY,
    coin_id VARCHAR NOT NULL UNIQUE, -- 'bitcoin'
    symbol VARCHAR NOT NULL, -- 'btc'
    name VARCHAR NOT NULL, -- 'Bitcoin'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS time_intervals (
    time_key BIGINT PRIMARY KEY, -- YYYYMMDDHHMM
    timestamp TIMESTAMP NOT NULL,
    date DATE NOT NULL,
    hour INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS crypto_prices (
    price_id SERIAL PRIMARY KEY,
    coin_key INTEGER NOT NULL,
    time_key BIGINT NOT NULL,
    price_usd NUMERIC NOT NULL,
    market_cap NUMERIC,
    volume_24h NUMERIC,
    price_change_pct_24h NUMERIC,
    collected_at TIMESTAMP NOT NULL,

    FOREIGN KEY (coin_key) REFERENCES coins(coin_key),
    FOREIGN KEY (time_key) REFERENCES time_intervals(time_key),

    UNIQUE(coin_key, time_key)  -- prevents duplicate price records for same coin at same timestamp
);
