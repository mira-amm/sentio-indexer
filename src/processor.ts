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
import { InputType, OutputType, Input, bn, ReceiptType, ZeroBytes32 } from 'fuels';
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

  return '0x' + crypto.createHash('sha256').update(contractBuffer).update(subId).digest('hex');
}

const processor = AmmProcessor.bind({
  address: AMM_CONTRACT_ADDRESS,
  chainId: FuelNetwork.TEST_NET,
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


FuelGlobalProcessor
  .bind({
    chainId: FuelNetwork.TEST_NET,
  })
  .onTransaction(
    async (tx, ctx) => {
      const txDate = tx.date ? normalizeTxDate(tx.date) : null;

      const assetsBalancesDiffs: Record<string, Record<string, bigint>> = {};

      if (tx.status === 'success') {
        for (const receipt of tx.receipts) {
          if (receipt.type === ReceiptType.LogData) {
            // Check for SRC20 metadata events
          } else if (receipt.type === ReceiptType.Transfer || (receipt.type === ReceiptType.Call && receipt.amount.lt(bn(0)))) {
            const assetsBaseOwner = assetsBalancesDiffs[receipt.to] ?? {};
            const assetsBaseOwnerBalance = assetsBaseOwner[receipt.assetId] ?? 0n;
            assetsBaseOwner[receipt.assetId] = assetsBaseOwnerBalance + BigInt(receipt.amount.toHex());
            assetsBalancesDiffs[receipt.to] = assetsBaseOwner;

            if (receipt.from !== ZeroBytes32) {
              const ownerDiff = assetsBalancesDiffs[receipt.from] ?? {};
              const ownerDiffBalance = ownerDiff[receipt.assetId] ?? 0n;

              ownerDiff[receipt.assetId] = ownerDiffBalance - BigInt(receipt.amount.toHex());
              assetsBalancesDiffs[receipt.from] = ownerDiff;
            }
          }
        }
      }

      const outputs = tx.transaction.outputs || [];
      const inputs = tx.transaction.inputs || [];

      for (const output of outputs) {
        switch (output.type) {
          case OutputType.Coin:
          case OutputType.Change:
          case OutputType.Variable:
            const assetsBaseOwner = assetsBalancesDiffs[output.to] ?? {};
            const assetsBaseOwnerBalance = assetsBaseOwner[output.assetId] ?? 0n;
            assetsBaseOwner[output.assetId] = assetsBaseOwnerBalance + BigInt(output.amount.toHex());
            assetsBalancesDiffs[output.to] = assetsBaseOwner;
            break;
        }
      }
      for (const input of inputs) {
        switch (input.type) {
          case InputType.Coin:
            const ownerDiff = assetsBalancesDiffs[input.owner] ?? {};
            const ownerDiffBalance = ownerDiff[input.assetId] ?? 0n;

            ownerDiff[input.assetId] = ownerDiffBalance - BigInt(input.amount.toHex());
            assetsBalancesDiffs[input.owner] = ownerDiff;
            break;
          case InputType.Message:
            const ownerDiffM = assetsBalancesDiffs[input.recipient] ?? {};
            const ownerDiffBalanceM = ownerDiffM[BASE_ASSET_ID] ?? 0n;

            ownerDiffM[BASE_ASSET_ID] = ownerDiffBalanceM - BigInt(input.amount.toHex());
            assetsBalancesDiffs[input.recipient] = ownerDiffM;

            // If message coin has never been seen before log as a balance change positive
            const amount = input.amount.toString();
            ctx.eventLogger.emit('assetBalance', {
              distinctId: input.recipient,
              txDate,
              assetId: BASE_ASSET_ID,
              amount,
            });

            break;
        }
      }

      // Emit all asset balance diffs
      Object.entries(assetsBalancesDiffs).forEach(([owner, assetsBalances]) => {
        Object.entries(assetsBalances).forEach(([assetId, assetBalanceDiff]) => {
          if (assetBalanceDiff === 0n) {
            ctx.eventLogger.emit('assetBalance', {
              distinctId: owner,
              txDate,
              assetId,
              amount: assetBalanceDiff.toString(),
            });
          }
        });
      });
    },
  );
