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
time_range AS (
    SELECT
        addHours(toDate(start_date), number) AS hour
    FROM (
        SELECT toDate(min(m.timestamp)) AS start_date FROM Mint m  -- Start date from the first mint event
    ) as date_range
    ARRAY JOIN range(dateDiff('hour', start_date, today())) AS number
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

pools AS (
    SELECT
        pc.poolId AS pool_id,
        pc.lpAssetId AS lp_token,  -- Use lpAssetId as the pool address
        pc.token0 AS token_0_address,
        pc.token1 AS token_1_address,
        a0.symbol AS token_0_symbol,
        a1.symbol AS token_1_symbol,
        a0.decimals AS token_0_decimals,
        a1.decimals AS token_1_decimals,
        '9889' AS chain_id
    FROM PairCreated pc
    JOIN VerifiedAsset a0 ON pc.token0 = a0.assetId
    JOIN VerifiedAsset a1 ON pc.token1 = a1.assetId
),

hourly_pools AS (
    SELECT
        time.hour,
        pools.*
    FROM time_range time
    CROSS JOIN pools
),

mint_agg AS (
    SELECT
        toStartOfHour(timestamp) AS hour,
        poolId,
        sum(liquidity) AS total_minted,
        SUM(token0In) AS token_0_in,
        SUM(token1In) AS token_1_in
    FROM
        Mint
    GROUP BY
        hour, poolId
),

burn_agg AS (
    SELECT
        toStartOfHour(timestamp) AS hour,
        poolId,
        sum(liquidity) AS total_burned,
        SUM(token0Out) AS token_0_out,
        SUM(token1Out) AS token_1_out
    FROM
        Burn
    GROUP BY
        hour, poolId
),

swap_agg AS (
    SELECT
        s.poolId,
        toStartOfHour(s.timestamp) AS hour,
        SUM(s.token0In) AS token_0_in,
        SUM(s.token1In) AS token_1_in,
        SUM(s.token0Out) AS token_0_out,
        SUM(s.token1Out) AS token_1_out
    FROM Swap s
    GROUP BY hour, poolId
),

hourly_pool_aggregated AS (
    SELECT
        p.hour,
        p.pool_id AS pool_id,
        p.lp_token,
        COALESCE(m.total_minted, 0) AS total_minted,
        COALESCE(b.total_burned, 0) AS total_burned,
        (COALESCE(m.total_minted, 0) - COALESCE(total_burned, 0)) AS net_minted,
        p.token_0_symbol,
        p.token_1_symbol,
        COALESCE(pr0.price, 0) AS token_0_price,
        COALESCE(pr1.price, 0) AS token_1_price,
        (COALESCE(m.token_0_in, 0) + COALESCE(s.token_0_in) - COALESCE(b.token_0_out) - COALESCE(s.token_0_out)) / POW(10, p.token_0_decimals) AS token_0_in,
        (COALESCE(m.token_1_in, 0) + COALESCE(s.token_1_in) - COALESCE(b.token_1_out) - COALESCE(s.token_1_out)) / POW(10, p.token_1_decimals) AS token_1_in
    FROM
        hourly_pools p
    LEFT JOIN
        mint_agg m ON p.hour = m.hour AND p.pool_id = m.poolId
    LEFT JOIN
        burn_agg b ON p.hour = b.hour AND p.pool_id = b.poolId
    LEFT JOIN
        swap_agg s ON p.hour = s.hour AND p.pool_id = s.poolId
    LEFT JOIN
        hourly_prices pr0 ON LOWER(p.token_0_symbol) = pr0.symbol AND p.hour = pr0.price_hour
    LEFT JOIN
        hourly_prices pr1 ON LOWER(p.token_1_symbol) = pr1.symbol AND p.hour = pr1.price_hour
    ORDER BY
        p.hour, p.pool_id
),

hourly_pool_snapshot AS (
    SELECT
        p.hour AS hour,
        p.pool_id AS pool_id,
        p.lp_token AS pool_address,
        SUM(p.net_minted) OVER (PARTITION BY p.pool_id ORDER BY hour) AS lp_supply,
        SUM(p.token_0_in) OVER (PARTITION BY p.pool_id ORDER BY hour) AS token_0_reserves,
        SUM(p.token_1_in) OVER (PARTITION BY p.pool_id ORDER BY hour) AS token_1_reserves,
        p.token_0_price,
        p.token_1_price,
    FROM
        hourly_pool_aggregated p
    ORDER BY
        p.hour, p.pool_id
),

hourly_pool_snapshot_priced AS (
    SELECT
        p.*,
        p.token_0_reserves * p.token_0_price AS token_0_value,
        p.token_1_reserves * p.token_1_price AS token_1_value,
        p.token_0_reserves * p.token_0_price + p.token_1_reserves * p.token_1_price AS total_value,
        (p.token_0_reserves * p.token_0_price + p.token_1_reserves * p.token_1_price) / p.lp_supply AS price_per_lp
    FROM
        hourly_pool_snapshot p
    WHERE
        p.lp_supply > 0 -- Otherwise we get divide by 0 errors
),
lp_tokens AS (
    SELECT
        pc.lpAssetId AS pool_address,  -- Use lpAssetId as the pool address
        pc.token0 AS token_0_address,
        pc.token1 AS token_1_address,
        '9889' AS chain_id  -- Assuming chain_id is constant
    FROM PairCreated pc
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
)

SELECT
    cb.user_address,
    cb.pool_address,
    cb.hour,
    cb.current_liquidity_position * hps.price_per_lp AS user_score
FROM cumulative_balances cb
JOIN hourly_pool_snapshot_priced hps
ON cb.pool_address = hps.pool_address AND cb.hour = hps.hour
ORDER BY cb.user_address, cb.pool_address, cb.hour;