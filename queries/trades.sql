WITH swap_events AS (
    SELECT
        s.timestamp AS timestamp,
        '9889' AS chain_id,  -- Assuming a standard chain ID
        s.block_number,
        s.log_index,
        s.transaction_hash,
        s.address AS pool_address,
        s.recipient AS taker_address,
        -- Assumption: transaction signer is not directly stored in Swap, using recipient as user address for simplicity
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
        s.token0Out + s.token1Out AS output_token_amount
    FROM Swap s
),
pair_info AS (
    SELECT
        pc.address AS pool_address,
        pc.token0 AS token_0_address,
        pc.token1 AS token_1_address
    FROM PairCreated pc
)
SELECT
    toUnixTimestamp(swap.timestamp) AS timestamp,
    swap.chain_id,
    swap.block_number,
    COALESCE(swap.log_index, ROW_NUMBER() OVER (PARTITION BY swap.transaction_hash ORDER BY swap.timestamp)) AS log_index,
    swap.transaction_hash,
    swap.user_address,
    swap.taker_address,
    swap.pool_address,
    CASE WHEN swap.input_token = 'token0' THEN pair.token_0_address ELSE pair.token_1_address END AS input_token_address,
    swap.input_token_amount,
    CASE WHEN swap.output_token = 'token0' THEN pair.token_0_address ELSE pair.token_1_address END AS output_token_address,
    swap.output_token_amount,
    CASE
        WHEN swap.input_token = 'token0' THEN swap.output_token_amount / NULLIF(swap.input_token_amount, 0)
        ELSE swap.input_token_amount / NULLIF(swap.output_token_amount, 0)
    END AS spot_price_after_swap,
    NULL AS swap_amount_usd,  -- Placeholder for USD calculation, requires external data
    NULL AS fees_usd  -- Placeholder for fee calculation, requires external data
FROM swap_events swap
LEFT JOIN pair_info pair ON swap.pool_address = pair.pool_address
ORDER BY swap.timestamp, swap.pool_address;
