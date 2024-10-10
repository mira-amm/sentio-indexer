WITH swap_events AS (
    SELECT
        s.timestamp AS timestamp,
        '9889' AS chain_id,  -- Assuming a standard chain ID
        s.block_number,
        s.log_index,
        s.transaction_hash,
        s.address AS pool_address,
        s.recipient AS taker_address,
        COALESCE(s.recipient, '') AS user_address,
        CASE
            WHEN s.token0In > 0 THEN 'token0'
            ELSE 'token1'
        END AS input_token,
        CASE
            WHEN s.token0Out > 0 THEN 'token0'
            ELSE 'token1'
        END AS output_token,
        s.token0In + s.token1In AS input_token_amount,
        s.token0Out + s.token1Out AS output_token_amount,
        toStartOfHour(s.timestamp) AS trade_hour  -- Round trade timestamp to the nearest hour
    FROM Swap s
),
pair_info AS (
    SELECT
        pc.address AS pool_address,
        pc.token0 AS token_0_address,
        pc.token1 AS token_1_address,
        CASE WHEN pc.stable = 0 THEN 0.03 ELSE 0.005 END AS pool_fee
    FROM PairCreated pc
),
token_info AS (
    SELECT
        va.assetId AS token_address,
        LOWER(va.symbol) AS token_symbol,
        va.decimals  -- Number of decimals to adjust the token amount
    FROM VerifiedAsset va
),
hourly_prices AS (
    SELECT
        p.symbol,
        p.price,
        toStartOfHour(p.time) AS price_hour,  -- Round price timestamp to the nearest hour
        ROW_NUMBER() OVER (PARTITION BY p.symbol, toStartOfHour(p.time) ORDER BY p.time DESC) AS row_num  -- Select one price per hour per symbol
    FROM __prices__ p
    WHERE p.time > toDateTime64('2024-08-01 00:00:00', 6, 'UTC')
)
SELECT
    toUnixTimestamp(swap.timestamp) AS timestamp,
    swap.chain_id,
    swap.block_number,
    COALESCE(swap.log_index, ROW_NUMBER() OVER (PARTITION BY swap.transaction_hash ORDER BY swap.timestamp)) AS log_index,
    swap.transaction_hash,
    swap.user_address,
    swap.taker_address,
    'MIRA-LP' as pair_name,
    swap.pool_address AS pool_address,
    ti.token_symbol as input_token_symbol,
    ti.token_address AS input_token_address,
    swap.input_token_amount / POW(10, ti.decimals) AS input_token_amount,  -- Adjusted input token amount
    to.token_symbol as output_token_symbol,
    to.token_address AS output_token_address,
    swap.output_token_amount / POW(10, ti.decimals) AS output_token_amount,  -- Adjusted output token amount
    CASE
        WHEN swap.input_token = 'token0' THEN swap.output_token_amount / NULLIF(swap.input_token_amount, 0)
        ELSE swap.input_token_amount / NULLIF(swap.output_token_amount, 0)
    END AS spot_price_after_swap,
    COALESCE((swap.input_token_amount / POW(10, ti.decimals)) * hp.price, 0) AS swap_amount_usd,  -- Adjust input amount by decimals and calculate USD value
    COALESCE((swap.input_token_amount / POW(10, ti.decimals)) * hp.price * pair.pool_fee, 0) AS fees_usd  -- Adjust input amount by decimals and calculate fees
FROM swap_events swap
LEFT JOIN pair_info pair ON swap.pool_address = pair.pool_address
LEFT JOIN token_info ti ON CASE WHEN swap.input_token = 'token0' THEN pair.token_0_address ELSE pair.token_1_address END = ti.token_address
LEFT JOIN token_info to ON CASE WHEN swap.input_token = 'token0' THEN pair.token_1_address ELSE pair.token_0_address END = to.token_address
LEFT JOIN (
    SELECT symbol, price, price_hour
    FROM hourly_prices
    WHERE row_num = 1  -- Select the most recent price within the same hour
) hp ON ti.token_symbol = LOWER(hp.symbol) AND swap.trade_hour = hp.price_hour  -- Match trades and prices by hour
ORDER BY swap.timestamp, swap.pool_address;
