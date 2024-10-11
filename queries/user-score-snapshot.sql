-- Query below gets cumulative balances of lp token for each user
-- for each pool for all time.
WITH RECURSIVE hours as (
    SELECT
       assetId,
       distinct_id,
       MIN(DATE_TRUNC('hour', timestamp)) AS hour
    FROM assetBalance
    GROUP BY distinct_id, assetId

    UNION ALL

    SELECT
        assetId,
        distinct_id,
        uh.hour + INTERVAL '1 hour'
    FROM hours uh
    WHERE uh.hour + INTERVAL '1 hour' <= CURRENT_TIMESTAMP()   
),
hourly_prices AS (
    SELECT
        p.symbol,
        argMin(price, time) AS price,
        toStartOfHour(min(time)) AS price_hour
    FROM __prices__ p
    INNER JOIN VerifiedAsset va ON p.symbol = lower(va.symbol)
    WHERE p.time > toDateTime64('2024-08-01 00:00:00', 6, 'UTC')
    GROUP BY p.symbol, toStartOfHour(p.time)
),
lp_tokens AS (
    SELECT
        pc.lpAssetId AS pool_address,  -- Use lpAssetId as the pool address
        pc.token0 AS token_0_address,
        pc.token1 AS token_1_address,
        a0.symbol AS token_0_symbol,
        a1.symbol AS token_1_symbol,
        '9889' AS chain_id  -- Assuming chain_id is constant
    FROM PairCreated pc
    JOIN VerifiedAsset a0 ON pc.token0 = a0.assetId
    JOIN VerifiedAsset a1 ON pc.token1 = a1.assetId
),
parsed_balances AS (
    SELECT
        ab.distinct_id AS user_address,
        ab.assetId AS asset_id,
        DATE_TRUNC('hour', ab.timestamp) AS hour,          -- Truncated hour
        ab.block_number AS block_number,
        CAST(ab.amount AS Decimal(38, 18)) AS delta_amount  -- Convert amount to Decimal
    FROM assetBalance ab
    WHERE ab.assetId IN (SELECT pool_address FROM lp_tokens)  -- Only include LP tokens
),
hourly_balances AS (
    SELECT
        pb.user_address,
        pb.asset_id AS pool_address,
        pb.hour,
        SUM(pb.delta_amount) AS hourly_delta
    FROM parsed_balances pb
    GROUP BY pb.user_address, pb.asset_id, pb.hour
),
hourly_balances_filled AS (
    SELECT
        h.distinct_id as user_address,
        h.hour,
        COALESCE(h.assetId, hb.pool_address) as pool_address,
        COALESCE(hb.hourly_delta, 0.0) AS hourly_delta
    FROM hours h
    LEFT JOIN hourly_balances hb
    ON
        h.distinct_id = hb.user_address AND
        h.hour = hb.hour AND
        h.assetId = hb.pool_address
),
cumulative_balances AS (
    SELECT
        user_address,
        pool_address,
        hour,
        SUM(hourly_delta) OVER (PARTITION BY user_address, pool_address ORDER BY hour) AS current_liquidity_position
    FROM hourly_balances_filled
),
priced_balances AS (
    SELECT
        cb.user_address,
        cb.pool_address,
        cb.hour,
        cb.current_liquidity_position,
        lt.token_0_symbol AS token_0_symbol,
        lt.token_1_symbol AS token_1_symbol,
        p0.price AS token_0_price,
        p1.price AS token_1_price
        -- COALESCE(va0.price, 0.0) AS token_0_price,
        -- COALESCE(va1.price, 0.0) AS token_1_price
    FROM cumulative_balances cb
    JOIN lp_tokens lt ON cb.pool_address = lt.pool_address
    JOIN hourly_prices p0 on lt.token_0_symbol = p0.symbol and cb.hour = p0.price_hour
    JOIN hourly_prices p1 on lt.token_1_symbol = p1.symbol and cb.hour = p1.price_hour
)

select * from priced_balances
WHERE current_liquidity_position > 0
ORDER BY hour asc;
