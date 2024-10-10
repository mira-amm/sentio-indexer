WITH date_range AS (
    SELECT
        addDays(toDate(start_date), number) AS day
    FROM (
        SELECT toDate(min(m.timestamp)) AS start_date FROM Mint m  -- Start date from the first mint event
    ) as date_range
    ARRAY JOIN range(dateDiff('day', start_date, today())) AS number
),
pair_created AS (
    SELECT
        pc.poolId AS pool_address,
        pc.token0 AS token_0_address,
        pc.token1 AS token_1_address,
        CASE WHEN pc.stable = 1 THEN 0.0005 ELSE 0.003 END AS fee_rate  -- Set fee rate based on stability
    FROM PairCreated pc
),
pair_tokens AS (
    SELECT
        pc.pool_address,
        pc.token_0_address AS token_address,
        0 AS token_index,
        pc.fee_rate
    FROM pair_created pc

    UNION ALL

    SELECT
        pc.pool_address,
        pc.token_1_address AS token_address,
        1 AS token_index,
        pc.fee_rate
    FROM pair_created pc
),
daily_mint_inflows AS (
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
        d.day AS day,
        p.pool_address as pool_address,
        p.token_address,
        p.token_index,
        p.fee_rate AS fee_rate,
        CASE WHEN p.token_index = 0
            THEN COALESCE(mint.token_0_inflow, 0) - COALESCE(burn.token_0_outflow) - COALESCE(swap.token_0_outflow, 0) + COALESCE(swap.token_0_inflow, 0)
            ELSE COALESCE(mint.token_1_inflow, 0) - COALESCE(burn.token_0_outflow) - COALESCE(swap.token_1_outflow, 0) + COALESCE(swap.token_1_inflow, 0)
        END as net_flow
    FROM date_range d
    CROSS JOIN pair_tokens p
    FULL OUTER JOIN daily_mint_inflows mint
        ON mint.day = d.day AND mint.pool_address = p.pool_address
    FULL OUTER JOIN daily_burn_outflows burn 
        ON burn.day = d.day AND burn.pool_address = p.pool_address
    FULL OUTER JOIN daily_swap_flows swap 
        ON swap.day = d.day AND swap.pool_address = p.pool_address
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
        argMin(price, time) AS price,
        toStartOfHour(min(time)) AS price_hour
    FROM __prices__ p
    INNER JOIN VerifiedAsset va ON p.symbol = lower(va.symbol)
    WHERE p.time > toDateTime64('2024-08-01 00:00:00', 6, 'UTC')
    GROUP BY p.symbol, toStartOfHour(p.time)
)


SELECT
    toUnixTimestamp(dnf.day) AS timestamp, -- Day as Unix timestamp
    formatDateTime(dnf.day, '%Y-%m-%d') AS block_date, -- Day in YYYY-MM-DD format
    '9889' AS chain_id, -- Hardcoded chain_id
    dnf.pool_address,
    dnf.token_index,
    dnf.token_address AS token_address,
    dnf.fee_rate,  -- Fee rate
    0 AS protocol_fees_usd,  -- Dummy column for now
    dnf.net_flow AS net_flow

FROM daily_net_flows dnf  -- Generate snapshots for every date
-- LEFT JOIN daily_net_flows dnf ON d.day = dnf.day AND p.pool_address = dnf.pool_address
-- -- LEFT JOIN token_info ti ON p.token_address = ti.token_address
-- -- LEFT JOIN hourly_prices hp0 ON hp0.symbol = ti.token_symbol
-- ORDER BY d.day, p.pool_address;