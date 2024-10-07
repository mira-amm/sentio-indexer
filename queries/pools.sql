SELECT
    pc.chain AS chain_id,
    pc.block_number AS creation_block_number,
    pc.timestamp AS timestamp,
    pc.lpAssetId AS pool_address,
    pc.lpAssetId AS lp_token_address,  -- Using lpAssetId as the LP token address
    'MIRA-LP' AS lp_token_symbol,
    pc.token0 AS token_address,
    COALESCE(va.symbol, 'UNKNOWN') AS token_symbol,  -- Use symbol from VerifiedAsset table or 'UNKNOWN' if not found
    COALESCE(va.decimals, 0) AS token_decimals,  -- Use decimals from VerifiedAsset or default to 0
    0 AS token_index,  -- Index for token0
    CASE WHEN pc.stable = 0 THEN 0.003 ELSE 0.0 END AS fee_rate,
    'CPMM' AS dex_type  -- Constant DEX type
FROM PairCreated pc
LEFT JOIN VerifiedAsset va ON pc.token0 = va.assetId  -- Join with VerifiedAsset for token0

UNION ALL

SELECT
    pc.chain AS chain_id,
    pc.block_number AS creation_block_number,
    pc.timestamp AS timestamp,
    pc.lpAssetId AS pool_address,
    pc.lpAssetId AS lp_token_address,  -- Using lpAssetId as the LP token address
    'MIRA-LP' AS lp_token_symbol,
    pc.token1 AS token_address,
    COALESCE(va.symbol, 'UNKNOWN') AS token_symbol,  -- Use symbol from VerifiedAsset table or 'UNKNOWN' if not found
    COALESCE(va.decimals, 0) AS token_decimals,  -- Use decimals from VerifiedAsset or default to 0
    1 AS token_index,  -- Index for token1
    CASE WHEN pc.stable = 0 THEN 0.03 ELSE 0.005 END AS fee_rate,
    'CPMM' AS dex_type  -- Constant DEX type
FROM PairCreated pc
LEFT JOIN VerifiedAsset va ON pc.token1 = va.assetId  -- Join with VerifiedAsset for token1;
