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
        pc.poolId AS pool_address,
        pc.token0 AS token_0_address,
        pc.token1 AS token_1_address,
        CASE WHEN pc.stable = 1 THEN 0.0005 ELSE 0.003 END AS fee_rate  -- Set fee rate based on stability
    FROM PairCreated pc
),
token_info AS (
    SELECT
        va.assetId AS token_address,
        LOWER(va.symbol) AS token_symbol,
        va.decimals
    FROM VerifiedAsset va
),
hourly_prices AS (
    SELECT
        p.symbol,
        p.price,
        toStartOfHour(p.time) AS price_hour,
        ROW_NUMBER() OVER (PARTITION BY p.symbol, toStartOfHour(p.time) ORDER BY p.time DESC) AS row_num
    FROM __prices__ p
    WHERE time > toDateTime64('2024-08-01 00:00:00', 6, 'UTC')  -- Only get prices after Aug 2024 for performance reasons
)
SELECT
    toUnixTimestamp(dnf.day) AS timestamp, -- Day as Unix timestamp
    formatDateTime(dnf.day, '%Y-%m-%d') AS block_date, -- Day in YYYY-MM-DD format
    '9889' AS chain_id, -- Hardcoded chain_id
    dnf.pool_address,
    0 AS token_index,
    pc.token_0_address AS token_address,
    SUM(dnf.net_token_0_inflow) OVER (PARTITION BY dnf.pool_address ORDER BY dnf.day) / POW(10, ti0.decimals) AS token_amount, -- Adjusted cumulative amount for token0
    COALESCE(hp0.price * (SUM(dnf.net_token_0_inflow) OVER (PARTITION BY dnf.pool_address ORDER BY dnf.day) / POW(10, ti0.decimals)), 0) AS token_amount_usd,  -- USD value for token0
    dnf.token_0_volume / POW(10, ti0.decimals) AS volume_amount, -- Adjusted volume for token0
    COALESCE(hp0.price * (dnf.token_0_volume / POW(10, ti0.decimals)), 0) AS volume_usd, -- USD value for volume of token0
    pc.fee_rate,  -- Fee rate
    COALESCE(hp0.price * (dnf.token_0_volume / POW(10, ti0.decimals)), 0) * pc.fee_rate AS total_fees_usd, -- Total fees in USD
    COALESCE(hp0.price * (dnf.token_0_volume / POW(10, ti0.decimals)), 0) * pc.fee_rate AS user_fees_usd, -- Total fees in USD
    0 as protocol_fees_usd
FROM daily_net_flows dnf
JOIN pair_created pc ON dnf.pool_address = pc.pool_address
LEFT JOIN token_info ti0 ON pc.token_0_address = ti0.token_address
LEFT JOIN (
    SELECT symbol, price, price_hour
    FROM hourly_prices
    WHERE row_num = 1
) hp0 ON LOWER(hp0.symbol) = ti0.token_symbol AND toStartOfHour(toDateTime(dnf.day)) = hp0.price_hour

UNION ALL

SELECT
    toUnixTimestamp(dnf.day) AS timestamp, -- Day as Unix timestamp
    formatDateTime(dnf.day, '%Y-%m-%d') AS block_date, -- Day in YYYY-MM-DD format
    '9889' AS chain_id, -- Hardcoded chain_id
    dnf.pool_address,
    1 AS token_index,
    pc.token_1_address AS token_address,
    SUM(dnf.net_token_1_inflow) OVER (PARTITION BY dnf.pool_address ORDER BY dnf.day) / POW(10, ti1.decimals) AS token_amount, -- Adjusted cumulative amount for token1
    COALESCE(hp1.price * (SUM(dnf.net_token_1_inflow) OVER (PARTITION BY dnf.pool_address ORDER BY dnf.day) / POW(10, ti1.decimals)), 0) AS token_amount_usd,  -- USD value for token1
    dnf.token_1_volume / POW(10, ti1.decimals) AS volume_amount, -- Adjusted volume for token1
    COALESCE(hp1.price * (dnf.token_1_volume / POW(10, ti1.decimals)), 0) AS volume_usd, -- USD value for volume of token1
    pc.fee_rate,  -- Fee rate
    COALESCE(hp1.price * (dnf.token_1_volume / POW(10, ti1.decimals)), 0) * pc.fee_rate AS total_fees_usd, -- Total fees in USD
    COALESCE(hp1.price * (dnf.token_1_volume / POW(10, ti1.decimals)), 0) * pc.fee_rate AS user_fees_usd, -- Total fees in USD
    0 as protocol_fees_usd
FROM daily_net_flows dnf
JOIN pair_created pc ON dnf.pool_address = pc.pool_address
LEFT JOIN token_info ti1 ON pc.token_1_address = ti1.token_address
LEFT JOIN (
    SELECT symbol, price, price_hour
    FROM hourly_prices
    WHERE row_num = 1
) hp1 ON LOWER(hp1.symbol) = ti1.token_symbol AND toStartOfHour(toDateTime(dnf.day)) = hp1.price_hour;
