WITH time_range AS (
    SELECT
        addHours(toDate(start_date), number) AS hour
    FROM (
        SELECT toDate(min(m.timestamp)) AS start_date FROM Mint m  -- Start date from the first mint event
    ) as date_range
    ARRAY JOIN range(dateDiff('hour', start_date, today())) AS number
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
        COALESCE(m.total_minted, 0) AS total_minted,
        COALESCE(b.total_burned, 0) AS total_burned,
        (COALESCE(m.total_minted, 0) - COALESCE(total_burned, 0)) AS net_minted,
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
    ORDER BY
        p.hour, p.pool_id
)

SELECT
    p.hour AS hour,
    p.pool_id AS pool_id,
    SUM(p.net_minted) OVER (PARTITION BY p.pool_id ORDER BY hour) AS lp_supply,
    SUM(p.token_0_in) OVER (PARTITION BY p.pool_id ORDER BY hour) AS token_0_reserves,
    SUM(p.token_1_in) OVER (PARTITION BY p.pool_id ORDER BY hour) AS token_1_reserves
FROM
    hourly_pool_aggregated p
ORDER BY
    p.hour, p.pool_id;
