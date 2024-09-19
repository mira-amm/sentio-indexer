// import { Counter } from '@sentio/sdk'
// import { ERC20Processor } from '@sentio/sdk/eth/builtin'

// const tokenCounter = Counter.register('token')

// const address = '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'

// ERC20Processor.bind({ address }).onEventTransfer(async (event, ctx) => {
//   const val = event.args.value.scaleDown(18)
//   tokenCounter.add(ctx, val)
// })


import { LogLevel, } from '@sentio/sdk'
import { FuelGlobalProcessor, FuelNetwork, FuelProcessor } from '@sentio/sdk/fuel';
import { InputType, OutputType, Input, bn, ReceiptType } from 'fuels';
import { AmmProcessor } from './types/fuel/AmmProcessor.js'
import { AssetIdInput } from './types/fuel/Amm.js';
import crypto from 'crypto';
import { AMM_CONTRACT_ADDRESS, BASE_ASSET_ID } from './const.js';
import { normalizeTxDate } from './utils.js';
import { Src20Processor } from './types/fuel/Src20Processor.js';
import { Src20Interface } from './types/fuel/Src20.js';

type PoolId = [AssetIdInput, AssetIdInput, boolean];

const poolIdToStr = (poolId: PoolId) => `${poolId[0].bits.slice(2)}-${poolId[1].bits.slice(2)}-${poolId[2]}`;

// const sha256 = (str: string) => crypto.createHash('sha256').update(str).digest('hex');

function getLPAssetId(poolId: PoolId) {
  const contractBuffer = Buffer.from(AMM_CONTRACT_ADDRESS.slice(2), 'hex');
  const subId = crypto.createHash('sha256')
    .update(Buffer.from(poolId[0].bits.slice(2), 'hex'))
    .update(Buffer.from(poolId[1].bits.slice(2), 'hex'))
    .update(Buffer.from(poolId[2] ? '01' : '00', 'hex'))
    .digest();

  return crypto.createHash('sha256').update(contractBuffer).update(subId).digest('hex');
}

const processor = AmmProcessor.bind({
  address: AMM_CONTRACT_ADDRESS,
  chainId: FuelNetwork.TEST_NET
});

processor.onLogCreatePoolEvent(async (event, ctx) => {
  ctx.meter.Counter('pools').add(1);
  ctx.eventLogger.emit("PairCreated", {
    poolId: poolIdToStr(event.data.pool_id),
    token0: event.data.pool_id[0].bits,
    token1: event.data.pool_id[1].bits,
    stable: event.data.pool_id[2],
    lpAssetId: getLPAssetId(event.data.pool_id),
  });
});

processor.onLogSwapEvent(async (event, ctx) => {
  ctx.eventLogger.emit("Swap", {
    poolId: poolIdToStr(event.data.pool_id),
    token0In: event.data.asset_0_in,
    token1In: event.data.asset_1_in,
    token0Out: event.data.asset_0_out,
    token1Out: event.data.asset_1_out,
    recipient: event.data.recipient.Address?.bits || event.data.recipient.ContractId?.bits,
  });
});

processor.onLogMintEvent(async (event, ctx) => {
  ctx.eventLogger.emit("Mint", {
    poolId: poolIdToStr(event.data.pool_id),
    token0In: event.data.asset_0_in,
    token1In: event.data.asset_1_in,
    liquidity: event.data.liquidity.amount,
    recipient: event.data.recipient.Address?.bits || event.data.recipient.ContractId?.bits,
    lpAssetId: event.data.liquidity.id.bits,
  });
});

processor.onLogBurnEvent(async (event, ctx) => {
  ctx.eventLogger.emit("Burn", {
    poolId: poolIdToStr(event.data.pool_id),
    token0Out: event.data.asset_0_out,
    token1Out: event.data.asset_1_out,
    liquidity: event.data.liquidity.amount,
    recipient: event.data.recipient.Address?.bits || event.data.recipient.ContractId?.bits,
    lpAssetId: event.data.liquidity.id.bits,
  });
});
