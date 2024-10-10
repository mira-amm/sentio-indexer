-- Get all hours since the start of time.
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
hourly_mint_inflows AS (
    SELECT
        poolId AS pool_address,
        DATE_TRUNC('hour', m.timestamp) AS hour,
        SUM(m.token0In) AS token_0_inflow,
        SUM(m.token1In) AS token_1_inflow
    FROM Mint m
    GROUP BY poolId, hour
),
hourly_burn_outflows AS (
    SELECT
        poolId AS pool_address,
        DATE_TRUNC('hour', b.timestamp) AS hour,
        SUM(b.token0Out) AS token_0_outflow,
        SUM(b.token1Out) AS token_1_outflow
    FROM Burn b
    GROUP BY poolId, hour
),
hourly_swap_flows AS (
    SELECT
        poolId AS pool_address,
        DATE_TRUNC('hour', s.timestamp) AS hour,
        SUM(s.token0In) AS token_0_inflow,
        SUM(s.token1In) AS token_1_inflow,
        SUM(s.token0Out) AS token_0_outflow,
        SUM(s.token1Out) AS token_1_outflow,
        SUM(s.token0In + s.token0Out) AS token_0_volume,  -- Calculate total volume for token0
        SUM(s.token1In + s.token1Out) AS token_1_volume   -- Calculate total volume for token1
    FROM Swap s
    GROUP BY poolId, hour
),
hourly_net_flows AS (
    SELECT
        h.hour AS hour,
        p.pool_address as pool_address,
        p.token_address,
        p.token_index,
        p.fee_rate AS fee_rate,
        CASE WHEN p.token_index = 0
            THEN COALESCE(mint.token_0_inflow, 0) - COALESCE(burn.token_0_outflow) - COALESCE(swap.token_0_outflow, 0) + COALESCE(swap.token_0_inflow, 0)
            ELSE COALESCE(mint.token_1_inflow, 0) - COALESCE(burn.token_0_outflow) - COALESCE(swap.token_1_outflow, 0) + COALESCE(swap.token_1_inflow, 0)
        END as net_flow
    FROM hours h
    CROSS JOIN pair_tokens p
    FULL OUTER JOIN hourly_mint_inflows mint
        ON mint.hour = h.hour AND mint.pool_address = p.pool_address
    FULL OUTER JOIN hourly_burn_outflows burn 
        ON burn.hour = h.hour AND burn.pool_address = p.pool_address
    FULL OUTER JOIN hourly_swap_flows swap 
        ON swap.hour = h.hour AND swap.pool_address = p.pool_address
),
token_info AS (
    SELECT
        va.assetId AS token_address,
        LOWER(va.symbol) AS token_symbol,
        va.decimals
    FROM VerifiedAsset va
),
hourly_timestamps as (
    SELECT
        symbol,
        date_add(hour, 1, date_trunc('hour', time)) as hour,
        MAX(time) as max_time
    FROM __prices__
    GROUP BY symbol, date_trunc('hour', time)
),
hourly_prices as (
    SELECT LOWER(pt.symbol) as symbol, pt.price, hourly_timestamps.hour
    FROM __prices__ AS pt
    JOIN hourly_timestamps ON
        hourly_timestamps.symbol = pt.symbol AND
        hourly_timestamps.max_time = pt.time
),
-- Attach price and symbol info to hourly net flows
hourly_token_net_flows AS (
    SELECT
        hnf.hour as hour,
        hnf.pool_address,
        hnf.fee_rate,  -- Fee rate
        0 AS protocol_fees_usd,  -- Dummy column for now
        hnf.net_flow AS net_flow,
        hnf.token_address,
        hnf.token_index,
        ti.token_symbol,
        ti.decimals,
        hp.price
    FROM hourly_net_flows hnf
    JOIN token_info ti
    ON hnf.token_address = ti.token_address
    JOIN hourly_prices hp
    ON ti.token_symbol = hp.symbol AND hnf.hour = hp.hour
),
-- Calculate net flows in USD at the pool level
hourly_pool_net_flows_usd as (
    SELECT
        htnf.hour,
        htnf.pool_address,
        AVG(htnf.fee_rate) as avg_fee_rate,
        AVG(htnf.protocol_fees_usd) as protocol_fees_usd,
        SUM(htnf.net_flow / POWER(10, htnf.decimals) * htnf.price) AS net_flow_usd
    FROM hourly_token_net_flows htnf
    GROUP BY hour, pool_address
)

-- Turn net flows into cumulative net flows
SELECT
    hour,
    pool_address,
    avg_fee_rate,
    protocol_fees_usd,
    SUM(net_flow_usd) OVER (PARTITION BY pool_address ORDER BY hour) as cumulative_net_flow_usd
FROM hourly_pool_net_flows_usd
ORDER BY hour, pool_address;


