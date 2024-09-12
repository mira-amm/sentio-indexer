// import { Counter } from '@sentio/sdk'
// import { ERC20Processor } from '@sentio/sdk/eth/builtin'

// const tokenCounter = Counter.register('token')

// const address = '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'

// ERC20Processor.bind({ address }).onEventTransfer(async (event, ctx) => {
//   const val = event.args.value.scaleDown(18)
//   tokenCounter.add(ctx, val)
// })


import { LogLevel } from '@sentio/sdk'
import { FuelNetwork, FuelProcessor } from '@sentio/sdk/fuel'
import { AmmProcessor } from './types/fuel/AmmProcessor.js'
import { AssetIdInput } from './types/fuel/Amm.js';

const contractAddress = '0xd5a716d967a9137222219657d7877bd8c79c64e1edb5de9f2901c98ebe74da80';

type PoolId = [AssetIdInput, AssetIdInput, boolean];

const poolIdToStr = (poolId: PoolId) => `${poolId[0]}-${poolId[1]}-${poolId[2]}`;

const processor = AmmProcessor.bind({
  address: contractAddress,
  chainId: FuelNetwork.TEST_NET
});

processor.onLogCreatePoolEvent(async (event, ctx) => {
  ctx.meter.Counter('pools').add(1);
  ctx.eventLogger.emit("PairCreated", {
    poolId: poolIdToStr(event.data.pool_id),
    token0: event.data.pool_id[0],
    token1: event.data.pool_id[1],
    stable: event.data.pool_id[2],
  });
});

processor.onLogSwapEvent(async (event, ctx) => {
  ctx.eventLogger.emit("Swap", {
    poolId: poolIdToStr(event.data.pool_id),
    token0In: event.data.asset_0_in,
    token1In: event.data.asset_1_in,
    token0Out: event.data.asset_0_out,
    token1Out: event.data.asset_1_out,
    recipient: event.data.recipient,
  });
});

processor.onLogMintEvent(async (event, ctx) => {
  ctx.eventLogger.emit("Mint", {
    poolId: poolIdToStr(event.data.pool_id),
    token0In: event.data.asset_0_in,
    token1In: event.data.asset_1_in,
    liquidity: event.data.liquidity,
    recipient: event.data.recipient,
  });
});

processor.onLogBurnEvent(async (event, ctx) => {
  ctx.eventLogger.emit("Burn", {
    poolId: poolIdToStr(event.data.pool_id),
    token0Out: event.data.asset_0_out,
    token1Out: event.data.asset_1_out,
    liquidity: event.data.liquidity,
    recipient: event.data.recipient,
  });
});
