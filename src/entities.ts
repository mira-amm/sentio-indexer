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

const HOUR = 60 * 60 * 1000;

// Note: caller must upsert both the pool and the returned snapshot
export async function getPoolSnapshot(pool: Pool, time: Date, ctx: FuelContractContext<Amm>): Promise<PoolSnapshot> {
  const snapshotTimestamp = Math.floor((time.getTime() - (time.getTime() % HOUR)) / 1000);
  const snapshotId = `${pool.id}-${snapshotTimestamp}`;

  if (pool.mostRecentSnapshot == snapshotTimestamp) {
    const _snapshot = await ctx.store.get(PoolSnapshot, snapshotId);
    if (!_snapshot) {
      throw new Error(`Missing snapshot ${snapshotId}`);
    }
    return _snapshot;
  } else {
    pool.mostRecentSnapshot = snapshotTimestamp;

    const snapshot = new PoolSnapshot({
      id: snapshotId,
      pool: Promise.resolve(pool),
      timestamp: snapshotTimestamp,

      reserve0: pool.reserve0,
      reserve1: pool.reserve1,

      volumeAsset0: pool.volumeAsset0,
      volumeAsset1: pool.volumeAsset1,
    });

    return snapshot;
  }
}