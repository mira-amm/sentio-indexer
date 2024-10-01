WITH lp_tokens AS (
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
        toUnixTimestamp(ab.timestamp) AS timestamp,  -- Full timestamp for daily snapshot
        toDate(ab.timestamp) AS block_date,          -- Truncated timestamp (YYYY-MM-DD format)
        ab.block_number AS block_number,
        CAST(ab.amount AS Decimal(38, 18)) AS delta_amount  -- Convert amount to Decimal
    FROM assetBalance ab
    WHERE ab.assetId IN (SELECT pool_address FROM lp_tokens)  -- Only include LP tokens
),
daily_balances AS (
    SELECT
        pb.user_address,
        pb.asset_id AS pool_address,
        pb.timestamp,
        pb.block_date,
        pb.block_number,
        SUM(pb.delta_amount) AS daily_delta
    FROM parsed_balances pb
    GROUP BY pb.user_address, pb.asset_id, pb.timestamp, pb.block_date, pb.block_number
),
cumulative_balances AS (
    SELECT
        user_address,
        pool_address,
        timestamp,
        block_date,
        block_number,
        SUM(daily_delta) OVER (PARTITION BY user_address, pool_address ORDER BY block_date) AS current_liquidity_position
    FROM daily_balances
)
SELECT
    cb.timestamp,  -- Full timestamp
    formatDateTime(cb.block_date, '%Y-%m-%d') AS block_date,  -- YYYY-MM-DD format
    lt.chain_id,  -- Chain ID from the LP token
    cb.block_number,
    cb.user_address,
    cb.pool_address,  -- LP token address as the pool address
    0.0 AS market_depth_score,  -- Placeholder for market depth score
    cb.current_liquidity_position AS total_value_locked_score  -- Use the cumulative liquidity position for TVL score
FROM cumulative_balances cb
JOIN lp_tokens lt ON cb.pool_address = lt.pool_address
WHERE cb.current_liquidity_position != 0  -- Only show non-zero positions
ORDER BY cb.timestamp, cb.pool_address;
