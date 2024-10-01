SELECT
    pc.chain AS chain_id,
    pc.block_number AS creation_block_number,
    pc.timestamp AS timestamp,
    pc.address AS pool_address,
    pc.lpAssetId AS lp_token_address, -- Using lpAssetId as the LP token address
    'MIRA-LP' AS lp_token_symbol,
    pc.token0 AS token_address,
    'UNKNOWN' AS token_symbol,  -- Placeholder since __coins__ table is unavailable
    0 AS token_decimals,  -- Defaulting to 0 since decimals are unavailable
    0 AS token_index,  -- Index for token0
    CASE WHEN pc.stable = 0 THEN 0.003 ELSE 0.0 END AS fee_rate,
    'CPMM' AS dex_type  -- Constant DEX type
FROM PairCreated pc

UNION ALL

SELECT
    pc.chain AS chain_id,
    pc.block_number AS creation_block_number,
    pc.timestamp AS timestamp,
    pc.address AS pool_address,
    pc.lpAssetId AS lp_token_address, -- Using lpAssetId as the LP token address
    'MIRA-LP' AS lp_token_symbol,
    pc.token1 AS token_address,
    'UNKNOWN' AS token_symbol,  -- Placeholder since __coins__ table is unavailable
    0 AS token_decimals,  -- Defaulting to 0 since decimals are unavailable
    1 AS token_index,  -- Index for token1
    CASE WHEN pc.stable = 0 THEN 0.003 ELSE 0.0 END AS fee_rate,
    'CPMM' AS dex_type  -- Constant DEX type
FROM PairCreated pc;
