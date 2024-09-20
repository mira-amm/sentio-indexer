SELECT
    pc.chain AS chain_id,
    pc.block_number AS creation_block_number,
    pc.timestamp AS timestamp,
    pc.address AS pool_address,
    pc.lpAssetId AS lp_token_address, -- Updated to reflect new lpAssetId type
    'MIRA-LP' AS lp_token_symbol,
    pc.token0 AS token_address,
    COALESCE(c0.symbol, 'UNKNOWN') AS token_symbol,
    COALESCE(c0.decimals, 0) AS token_decimals,
    0 AS token_index, -- Index for token0
    CASE WHEN pc.stable = 0 THEN 0.003 ELSE 0.0 END AS fee_rate,
    'CPMM' AS dex_type
FROM PairCreated pc
LEFT JOIN __coins__ c0 ON pc.token0 = c0.address AND pc.chain = c0.chain

UNION ALL

SELECT
    pc.chain AS chain_id,
    pc.block_number AS creation_block_number,
    pc.timestamp AS timestamp,
    pc.address AS pool_address,
    pc.lpAssetId AS lp_token_address, -- Updated to reflect new lpAssetId type
    'MIRA-LP' AS lp_token_symbol,
    pc.token1 AS token_address,
    COALESCE(c1.symbol, 'UNKNOWN') AS token_symbol,
    COALESCE(c1.decimals, 0) AS token_decimals,
    1 AS token_index, -- Index for token1
    CASE WHEN pc.stable = 0 THEN 0.003 ELSE 0.0 END AS fee_rate,
    'CPMM' AS dex_type
FROM PairCreated pc
LEFT JOIN __coins__ c1 ON pc.token1 = c1.address AND pc.chain = c1.chain;
