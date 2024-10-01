WITH lp_tokens AS (
    SELECT
        lpAssetId AS pool_address,  -- Use lpAssetId as the pool address
        token0 AS token_0_address,
        token1 AS token_1_address,
        '9889' AS chain_id  -- Assuming chain_id is constant, update if needed
    FROM PairCreated
),
parsed_balances AS (
    SELECT
        distinct_id AS user_address,
        assetId AS asset_id,
        toUnixTimestamp(timestamp) AS timestamp,  -- Full timestamp for snapshot
        toDate(timestamp) AS block_date,          -- Truncated timestamp (YYYY-MM-DD format)
        CAST(amount AS Decimal(38, 18)) AS delta_amount  -- Convert amount for summation
    FROM assetBalance
    WHERE assetId IN (SELECT pool_address FROM lp_tokens)  -- Filter to LP tokens
),
daily_balances AS (
    SELECT
        user_address,
        asset_id,
        timestamp,
        block_date,
        SUM(delta_amount) AS daily_delta
    FROM parsed_balances
    GROUP BY user_address, asset_id, timestamp, block_date
),
cumulative_balances AS (
    SELECT
        user_address,
        asset_id,
        timestamp,
        block_date,
        SUM(daily_delta) OVER (PARTITION BY user_address, asset_id ORDER BY block_date) AS current_balance
    FROM daily_balances
)
SELECT
    cb.timestamp,  -- Full timestamp
    formatDateTime(cb.block_date, '%Y-%m-%d') AS block_date,  -- YYYY-MM-DD format
    lt.chain_id,  -- Chain ID from the LP token
    cb.asset_id AS pool_address,  -- Use the asset_id as pool address (LPToken)
    cb.user_address,  -- Liquidity provider address
    CASE
        WHEN cb.asset_id = lt.token_0_address THEN 0
        ELSE 1
    END AS token_index,  -- Token index (0 for token0, 1 for token1)
    CASE
        WHEN cb.asset_id = lt.token_0_address THEN lt.token_0_address
        ELSE lt.token_1_address
    END AS token_address,  -- Token contract address
    cb.current_balance AS token_amount,  -- Current balance (liquidity position)
    NULL AS token_amount_usd  -- Placeholder for token amount in USD (optional, requires external data)
FROM cumulative_balances cb
JOIN lp_tokens lt ON cb.asset_id = lt.pool_address  -- Use LP token (asset_id) as the pool address
WHERE cb.current_balance != 0  -- Only show non-zero balances
ORDER BY cb.timestamp, cb.asset_id;
