import { FuelContractContext } from "@sentio/sdk/fuel";
import { Pool, PoolSnapshot } from "./schema/store.js";
import { Amm } from "./types/fuel/Amm.js";
import { getLPAssetId, PoolId, poolIdToStr } from "./utils.js";

export async function newPool(poolId: PoolId, ctx: FuelContractContext<Amm>) {
  const pool = new Pool({
    id: poolIdToStr(poolId),
    asset0: poolId[0].bits,
    asset1: poolId[1].bits,
    isStable: poolId[2],
    lpToken: getLPAssetId(poolId),

    lpTokenSupply: 0n,
    reserve0: 0n,
    reserve1: 0n,

    volumeAsset0: 0n,
    volumeAsset1: 0n,
  });
  ctx.store.upsert(pool);
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
      pool: Promise.resolve(pool),
      timestamp: currentSnapshotTimestamp,

      transactions: 0,

      reserve0: pool.reserve0,
      reserve1: pool.reserve1,

      lpTokenSupply: 0n,

      volumeAsset0: 0n,
      volumeAsset1: 0n,
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
        pool: Promise.resolve(pool),
        timestamp: timestamp,

        transactions: 0,

        reserve0: pool.reserve0,
        reserve1: pool.reserve1,
        reserve0Decimal: pool.reserve0Decimal,
        reserve1Decimal: pool.reserve1Decimal,

        lpTokenSupply: pool.lpTokenSupply,

        volumeAsset0: 0n,
        volumeAsset1: 0n,
      });
      await ctx.store.upsert(snapshot);
    }

    pool.mostRecentSnapshot = currentSnapshotTimestamp;

    return snapshot!;
  }
}