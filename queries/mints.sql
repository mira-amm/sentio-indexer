WITH mint_events AS (
    SELECT
        m.timestamp AS timestamp,
        '9889' AS chain_id,  -- Assuming a standard chain ID, update if needed
        m.block_number,
        m.log_index,
        m.transaction_hash,
        COALESCE(m.distinct_id, '') AS transaction_from_address,  -- Assumption: 'distinct_id' as the signer
        m.address AS from_address,  -- 'from' address of the event (address that sent the tokens)
        m.recipient AS to_address,  -- 'to' address (recipient of the LP tokens)
        pc.lpAssetId AS pool_address,  -- Using lpAssetId as the pool address
        pc.token0 AS token0_address,
        m.token0In AS token0_amount,  -- Amount of token0 provided during minting
        pc.token1 AS token1_address,
        m.token1In AS token1_amount,  -- Amount of token1 provided during minting
        m.liquidity AS mint_amount  -- Amount of LP tokens minted
    FROM Mint m
    JOIN PairCreated pc ON m.poolId = pc.poolId  -- Join Mint events with the pool information
)
SELECT
    toUnixTimestamp(mint.timestamp) AS timestamp,  -- Full timestamp as Unix
    mint.chain_id,  -- Chain ID (e.g., 9889)
    mint.block_number,
    COALESCE(mint.log_index, ROW_NUMBER() OVER (PARTITION BY mint.transaction_hash ORDER BY mint.timestamp)) AS log_index,  -- Generate log index if not available
    mint.transaction_hash,
    mint.transaction_from_address,  -- Address initiating the mint
    mint.from_address,  -- 'from' address for the mint event
    mint.to_address,  -- 'to' address for the LP tokens
    mint.pool_address,  -- LP token address representing the pool
    mint.token0_address,  -- Token0 contract address
    mint.token0_amount,  -- Amount of token0
    mint.token1_address,  -- Token1 contract address
    mint.token1_amount,  -- Amount of token1
    mint.mint_amount AS mint_amount,  -- Amount of LP tokens minted
    NULL AS mint_amount_usd  -- Placeholder for USD value, requires external pricing data
FROM mint_events mint
ORDER BY mint.timestamp, mint.pool_address;
