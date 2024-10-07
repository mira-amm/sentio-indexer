WITH mint_events AS (
    SELECT
        m.timestamp AS timestamp,
        '9889' AS chain_id,  -- Assuming a standard chain ID
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
        m.liquidity AS mint_amount,  -- Amount of LP tokens minted
        toStartOfHour(m.timestamp) AS mint_hour  -- Round timestamp to the nearest hour for price matching
    FROM Mint m
    JOIN PairCreated pc ON m.poolId = pc.poolId  -- Join Mint events with the pool information
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
    WHERE time > toDateTime64('2024-08-01 00:00:00', 6, 'UTC')  -- Only get prices after Aug 2024 for performance reasons
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
    mint.token0_amount / POW(10, ti0.decimals) AS token0_amount,  -- Adjusted amount of token0
    mint.token1_address,  -- Token1 contract address
    mint.token1_amount / POW(10, ti1.decimals) AS token1_amount,  -- Adjusted amount of token1
    mint.mint_amount / POW(10, 9) AS mint_amount,  -- Amount of LP tokens minted
    COALESCE(
        ((mint.token0_amount / POW(10, ti0.decimals)) * hp0.price) + 
        ((mint.token1_amount / POW(10, ti1.decimals)) * hp1.price), 0) AS mint_amount_usd  -- Calculate USD value using prices of token0 and token1
FROM mint_events mint
LEFT JOIN token_info ti0 ON mint.token0_address = ti0.token_address
LEFT JOIN token_info ti1 ON mint.token1_address = ti1.token_address
LEFT JOIN (
    SELECT symbol, price, price_hour
    FROM hourly_prices
    WHERE row_num = 1  -- Select the most recent price within the same hour
) hp0 ON LOWER(hp0.symbol) = ti0.token_symbol AND mint.mint_hour = hp0.price_hour  -- Match token0 price by hour
LEFT JOIN (
    SELECT symbol, price, price_hour
    FROM hourly_prices
    WHERE row_num = 1  -- Select the most recent price within the same hour
) hp1 ON LOWER(hp1.symbol) = ti1.token_symbol AND mint.mint_hour = hp1.price_hour  -- Match token1 price by hour
ORDER BY mint.timestamp, mint.pool_address;
