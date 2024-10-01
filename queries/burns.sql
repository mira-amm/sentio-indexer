WITH burn_events AS (
    SELECT
        b.timestamp AS timestamp,
        '9889' AS chain_id,  -- Assuming a standard chain ID, update if needed
        b.block_number,
        b.log_index,
        b.transaction_hash,
        COALESCE(b.distinct_id, '') AS transaction_from_address,  -- Assumption: 'distinct_id' as the signer
        b.address AS from_address,  -- 'from' address of the event (address that burned the LP tokens)
        b.recipient AS to_address,  -- 'to' address (recipient of the underlying tokens from the burn)
        pc.lpAssetId AS pool_address,  -- Using lpAssetId as the pool address
        pc.token0 AS token0_address,
        b.token0Out AS token0_amount,  -- Amount of token0 withdrawn during burning
        pc.token1 AS token1_address,
        b.token1Out AS token1_amount,  -- Amount of token1 withdrawn during burning
        b.liquidity AS burn_amount  -- Amount of LP tokens burned
    FROM Burn b
    JOIN PairCreated pc ON b.poolId = pc.poolId  -- Join Burn events with the pool information
)
SELECT
    toUnixTimestamp(burn.timestamp) AS timestamp,  -- Full timestamp as Unix
    burn.chain_id,  -- Chain ID (e.g., 9889)
    burn.block_number,
    COALESCE(burn.log_index, ROW_NUMBER() OVER (PARTITION BY burn.transaction_hash ORDER BY burn.timestamp)) AS log_index,  -- Generate log index if not available
    burn.transaction_hash,
    burn.transaction_from_address,  -- Address initiating the burn
    burn.from_address,  -- 'from' address for the burn event
    burn.to_address,  -- 'to' address receiving the underlying tokens
    burn.pool_address,  -- LP token address representing the pool
    burn.token0_address,  -- Token0 contract address
    burn.token0_amount,  -- Amount of token0 withdrawn
    burn.token1_address,  -- Token1 contract address
    burn.token1_amount,  -- Amount of token1 withdrawn
    burn.burn_amount AS burn_amount,  -- Amount of LP tokens burned
    NULL AS burn_amount_usd  -- Placeholder for USD value, requires external pricing data
FROM burn_events burn
ORDER BY burn.timestamp, burn.pool_address;
