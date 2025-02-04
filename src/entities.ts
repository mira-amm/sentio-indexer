import { FuelContractContext } from "@sentio/sdk/fuel";
import { Campaign, Pool, PoolSnapshot, Position } from "./schema/store.js";
import { Amm } from "./types/fuel/Amm.js";
import { getLPAssetId, PoolId, poolIdToStr } from "./utils.js";

export async function newPool(poolId: PoolId, ctx: FuelContractContext<Amm>) {
  const pool = new Pool({
    id: poolIdToStr(poolId),
    asset0: poolId[0].bits,
    asset1: poolId[1].bits,
    isStable: poolId[2],
    lpToken: getLPAssetId(poolId),
    lpTokenSupplyDecimal: 0,
    lpTokenSupply: 0n,
    reserve0: 0n,
    reserve1: 0n,
    reserve0Decimal: 0,
    reserve1Decimal: 0,
    volumeAsset0: 0n,
    volumeAsset1: 0n,
    volumeAsset0Decimal: 0,
    volumeAsset1Decimal: 0,
    mostRecentSnapshot: 0
  });
  await ctx.store.upsert(pool);
}

const HOUR_MS = 60 * 60 * 1000;
const HOUR_S = 60 * 60;

// Note: caller must upsert both the pool and the returned snapshot
export async function getPoolSnapshot(pool: Pool, time: Date, ctx: FuelContractContext<Amm>): Promise<PoolSnapshot> {
  const currentSnapshotTimestamp = Math.floor((time.getTime() - (time.getTime() % HOUR_MS)) / 1000);
  const currentSnapshotId = `${pool.id}-${currentSnapshotTimestamp}`;

  console.log(`Current snapshot timestamp: ${currentSnapshotTimestamp}, most recent snapshot: ${pool.mostRecentSnapshot}`);
  if (pool.mostRecentSnapshot == 0) {
    const snapshot = new PoolSnapshot({
      id: currentSnapshotId,
      poolId: pool.id,
      timestamp: currentSnapshotTimestamp,

      transactions: 0,

      reserve0: pool.reserve0,
      reserve1: pool.reserve1,
      reserve0Decimal: pool.reserve0Decimal,
      reserve1Decimal: pool.reserve1Decimal,

      lpTokenSupply: pool.lpTokenSupply,
      lpTokenSupplyDecimal: pool.lpTokenSupplyDecimal,
      volumeAsset0: 0n,
      volumeAsset1: 0n,
      volumeAsset0Decimal: pool.volumeAsset0Decimal,
      volumeAsset1Decimal: pool.volumeAsset1Decimal,

    });

    pool.mostRecentSnapshot = currentSnapshotTimestamp;

    return snapshot;
  } else if (pool.mostRecentSnapshot == currentSnapshotTimestamp) {
    const _snapshot = await ctx.store.get(PoolSnapshot, currentSnapshotId);
    if (!_snapshot) {
      throw new Error(`Missing snapshot ${currentSnapshotId}`);
    }
    return _snapshot;
  } else {
    let snapshot: PoolSnapshot;

    for (let timestamp = pool.mostRecentSnapshot; timestamp <= currentSnapshotTimestamp; timestamp += HOUR_S) {
      const snapshotId = `${pool.id}-${timestamp}`;
      snapshot = new PoolSnapshot({
        id: snapshotId,
        poolId: pool.id,
        timestamp: timestamp,

        transactions: 0,

        reserve0: pool.reserve0,
        reserve1: pool.reserve1,
        reserve0Decimal: pool.reserve0Decimal,
        reserve1Decimal: pool.reserve1Decimal,

        lpTokenSupply: pool.lpTokenSupply,
        lpTokenSupplyDecimal: pool.lpTokenSupplyDecimal,
        volumeAsset0: 0n,
        volumeAsset1: 0n,
        volumeAsset0Decimal: pool.volumeAsset0Decimal,
        volumeAsset1Decimal: pool.volumeAsset1Decimal,
      });
      await ctx.store.upsert(snapshot);
    }

    pool.mostRecentSnapshot = currentSnapshotTimestamp;

    return snapshot!;
  }
}
