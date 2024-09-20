WITH daily_mint_inflows AS (
    SELECT
        poolId AS pool_address,
        toDate(m.timestamp) AS day,
        SUM(m.token0In) AS token_0_inflow,
        SUM(m.token1In) AS token_1_inflow
    FROM Mint m
    GROUP BY poolId, toDate(m.timestamp)
),
daily_burn_outflows AS (
    SELECT
        poolId AS pool_address,
        toDate(b.timestamp) AS day,
        SUM(b.token0Out) AS token_0_outflow,
        SUM(b.token1Out) AS token_1_outflow
    FROM Burn b
    GROUP BY poolId, toDate(b.timestamp)
),
daily_swap_flows AS (
    SELECT
        poolId AS pool_address,
        toDate(s.timestamp) AS day,
        SUM(s.token0In) AS token_0_inflow,
        SUM(s.token1In) AS token_1_inflow,
        SUM(s.token0Out) AS token_0_outflow,
        SUM(s.token1Out) AS token_1_outflow,
        SUM(s.token0In + s.token0Out) AS token_0_volume,  -- Calculate total volume for token0
        SUM(s.token1In + s.token1Out) AS token_1_volume   -- Calculate total volume for token1
    FROM Swap s
    GROUP BY poolId, toDate(s.timestamp)
),
daily_net_flows AS (
    SELECT
        COALESCE(mint.pool_address, burn.pool_address, swap.pool_address) AS pool_address,
        COALESCE(mint.day, burn.day, swap.day) AS day,
        COALESCE(mint.token_0_inflow, 0) + COALESCE(swap.token_0_inflow, 0) - COALESCE(swap.token_0_outflow, 0) - COALESCE(burn.token_0_outflow, 0) AS net_token_0_inflow,
        COALESCE(mint.token_1_inflow, 0) + COALESCE(swap.token_1_inflow, 0) - COALESCE(swap.token_1_outflow, 0) - COALESCE(burn.token_1_outflow, 0) AS net_token_1_inflow,
        COALESCE(swap.token_0_volume, 0) AS token_0_volume,  -- Include total volume for token0
        COALESCE(swap.token_1_volume, 0) AS token_1_volume   -- Include total volume for token1
    FROM daily_mint_inflows mint
    FULL OUTER JOIN daily_burn_outflows burn 
        ON mint.pool_address = burn.pool_address 
        AND mint.day = burn.day
    FULL OUTER JOIN daily_swap_flows swap 
        ON mint.pool_address = swap.pool_address 
        AND mint.day = swap.day
        OR burn.pool_address = swap.pool_address 
        AND burn.day = swap.day
),
pair_created AS (
    SELECT
        poolId AS pool_address,
        token0 AS token_0_address,
        token1 AS token_1_address
    FROM PairCreated
)
SELECT
    toUnixTimestamp(day) AS timestamp, -- Day as Unix timestamp
    formatDateTime(day, '%Y-%m-%d') AS block_date, -- Day in YYYY-MM-DD format
    '9889' AS chain_id, -- Hardcoded chain_id
    pool_address,
    0 AS token_index,
    pc.token_0_address AS token_address,
    SUM(net_token_0_inflow) OVER (PARTITION BY pool_address ORDER BY day) AS token_amount, -- Cumulative amount of tokens for token0
    token_0_volume AS volume_amount -- Volume amount at the point in time
FROM daily_net_flows dnf
JOIN pair_created pc ON dnf.pool_address = pc.pool_address

UNION ALL

SELECT
    toUnixTimestamp(day) AS timestamp, -- Day as Unix timestamp
    formatDateTime(day, '%Y-%m-%d') AS block_date, -- Day in YYYY-MM-DD format
    '9889' AS chain_id, -- Hardcoded chain_id
    pool_address,
    1 AS token_index,
    pc.token_1_address AS token_address,
    SUM(net_token_1_inflow) OVER (PARTITION BY pool_address ORDER BY day) AS token_amount, -- Cumulative amount of tokens for token1
    token_1_volume AS volume_amount -- Volume amount at the point in time
FROM daily_net_flows dnf
JOIN pair_created pc ON dnf.pool_address = pc.pool_address;
