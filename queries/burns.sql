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
        b.liquidity AS burn_amount,  -- Amount of LP tokens burned
        toStartOfHour(b.timestamp) AS burn_hour  -- Round timestamp to the nearest hour for price matching
    FROM Burn b
    JOIN PairCreated pc ON b.poolId = pc.poolId  -- Join Burn events with the pool information
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
    burn.token0_amount / POW(10, ti0.decimals) AS token0_amount_adjusted,  -- Adjusted amount of token0
    burn.token1_address,  -- Token1 contract address
    burn.token1_amount / POW(10, ti1.decimals) AS token1_amount_adjusted,  -- Adjusted amount of token1
    burn.burn_amount / POW(10, 9) AS burn_amount,  -- Amount of LP tokens burned
    COALESCE(
        ((burn.token0_amount / POW(10, ti0.decimals)) * hp0.price) + 
        ((burn.token1_amount / POW(10, ti1.decimals)) * hp1.price), 0) AS burn_amount_usd  -- Calculate USD value using prices of token0 and token1
FROM burn_events burn
LEFT JOIN token_info ti0 ON burn.token0_address = ti0.token_address
LEFT JOIN token_info ti1 ON burn.token1_address = ti1.token_address
LEFT JOIN (
    SELECT symbol, price, price_hour
    FROM hourly_prices
    WHERE row_num = 1  -- Select the most recent price within the same hour
) hp0 ON LOWER(hp0.symbol) = ti0.token_symbol AND burn.burn_hour = hp0.price_hour  -- Match token0 price by hour
LEFT JOIN (
    SELECT symbol, price, price_hour
    FROM hourly_prices
    WHERE row_num = 1  -- Select the most recent price within the same hour
) hp1 ON LOWER(hp1.symbol) = ti1.token_symbol AND burn.burn_hour = hp1.price_hour  -- Match token1 price by hour
ORDER BY burn.timestamp, burn.pool_address;
