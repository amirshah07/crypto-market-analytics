-- latest prices for all coins
SELECT 
    c.coin_id, 
    c.name, 
    c.symbol, 
    cp.price_usd, 
    cp.market_cap, 
    cp.price_change_pct_24h, 
    ti.timestamp AS last_updated
FROM crypto_prices AS cp
INNER JOIN coins AS c 
ON cp.coin_key = c.coin_key
INNER JOIN time_intervals AS ti 
ON cp.time_key = ti.time_key
WHERE cp.time_key = (
    SELECT MAX(time_key)
    FROM crypto_prices
    WHERE coin_key = cp.coin_key
)
ORDER BY c.name;

-- top 10 gainers in last 24h
SELECT 
    c.name,
    c.symbol,
    cp.price_usd,
    cp.price_change_pct_24h,
    cp.market_cap
FROM crypto_prices AS cp
INNER JOIN coins AS c 
ON cp.coin_key = c.coin_key
WHERE cp.time_key = (
    SELECT MAX(time_key) 
    FROM crypto_prices
)
ORDER BY cp.price_change_pct_24h DESC
LIMIT 10;

-- top 10 losers in last 24h
SELECT 
    c.name,
    c.symbol,
    cp.price_usd,
    cp.price_change_pct_24h,
    cp.market_cap
FROM crypto_prices AS cp
INNER JOIN coins AS c 
ON cp.coin_key = c.coin_key
WHERE cp.time_key = (
    SELECT MAX(time_key) 
    FROM crypto_prices
)
ORDER BY cp.price_change_pct_24h
LIMIT 10;

-- price history for a specific coin over time (bitcoin is placeholder for now)
SELECT 
    c.name,
    c.symbol,
    cp.price_usd,
    cp.market_cap,
    cp.price_change_pct_24h,
    ti.timestamp
FROM crypto_prices AS cp
INNER JOIN coins AS c 
ON cp.coin_key = c.coin_key
INNER JOIN time_intervals AS ti
ON cp.time_key = ti.time_key
WHERE c.coin_id = 'bitcoin'
ORDER BY ti.timestamp;

--market cap rankings
SELECT 
    ROW_NUMBER() OVER (ORDER BY cp.market_cap DESC) AS rank,
    c.coin_id, 
    c.name, 
    c.symbol, 
    cp.price_usd, 
    cp.market_cap, 
    cp.price_change_pct_24h, 
    ti.timestamp AS last_updated
FROM crypto_prices AS cp
INNER JOIN coins AS c 
ON cp.coin_key = c.coin_key
INNER JOIN time_intervals AS ti 
ON cp.time_key = ti.time_key
WHERE cp.time_key = (
    SELECT MAX(time_key)
    FROM crypto_prices
    WHERE coin_key = cp.coin_key
)
ORDER BY cp.market_cap DESC;

-- average price by hour
SELECT
    c.coin_id,
    c.name,
    c.symbol,
    DATE_TRUNC('hour', ti.timestamp) AS hour,
    AVG(cp.price_usd) AS avg_price_usd
FROM crypto_prices AS cp
INNER JOIN coins AS c
ON cp.coin_key = c.coin_key
INNER JOIN time_intervals AS ti
ON cp.time_key = ti.time_key
GROUP BY
    c.coin_id,
    c.name,
    c.symbol,
    DATE_TRUNC('hour', ti.timestamp)
ORDER BY c.name, hour;