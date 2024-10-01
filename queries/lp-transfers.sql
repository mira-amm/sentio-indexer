WITH lp_tokens AS (
    SELECT
        lpAssetId AS pool_address  -- LP token contract address used as the pool address
    FROM PairCreated
),
negative_balance_events AS (
    SELECT
        ab.timestamp,
        ab.block_number,
        ab.log_index,
        ab.transaction_hash,
        ab.distinct_id AS from_address,
        ab.assetId AS pool_address,
        CAST(ab.amount AS Decimal(65, 30)) AS amount  -- Use larger precision for the Decimal type
    FROM assetBalance ab
    WHERE CAST(ab.amount AS Decimal(65, 30)) < 0  -- Negative balance events (from_address)
    AND ab.assetId IN (SELECT pool_address FROM lp_tokens)  -- Filter to LP tokens
),
positive_balance_events AS (
    SELECT
        ab.timestamp,
        ab.block_number,
        ab.log_index,
        ab.transaction_hash,
        ab.distinct_id AS to_address,
        ab.assetId AS pool_address,
        CAST(ab.amount AS Decimal(65, 30)) AS amount  -- Use larger precision for the Decimal type
    FROM assetBalance ab
    WHERE CAST(ab.amount AS Decimal(65, 30)) > 0  -- Positive balance events (to_address)
    AND ab.assetId IN (SELECT pool_address FROM lp_tokens)  -- Filter to LP tokens
),
transfers AS (
    SELECT
        neg.timestamp,
        '9889' AS chain_id,  -- Assuming a constant chain ID, adjust if necessary
        neg.block_number,
        COALESCE(neg.log_index, ROW_NUMBER() OVER (PARTITION BY neg.transaction_hash ORDER BY neg.timestamp)) AS log_index,  -- Generate log index if not available
        neg.transaction_hash,
        neg.from_address,
        pos.to_address,
        neg.pool_address,
        pos.amount AS pool_token_balance  -- Positive balance representing the transferred amount
    FROM negative_balance_events neg
    JOIN positive_balance_events pos
        ON neg.transaction_hash = pos.transaction_hash  -- Match by transaction hash
        AND neg.pool_address = pos.pool_address  -- Match by pool (LP token)
        AND neg.amount = -pos.amount  -- Ensure the amounts are inverse (transfer)
)
SELECT
    toUnixTimestamp(transfers.timestamp) AS timestamp,  -- Full timestamp as Unix
    transfers.chain_id,
    transfers.block_number,
    transfers.log_index,
    transfers.transaction_hash,
    transfers.from_address AS transaction_from_address,  -- From address as transaction initiator
    transfers.from_address,
    transfers.to_address,
    transfers.pool_address,  -- LP token contract address used as the pool address
    transfers.pool_token_balance  -- Transferred amount of LP tokens
FROM transfers
ORDER BY transfers.timestamp, transfers.pool_address;
