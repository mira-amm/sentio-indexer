import { InputType, Input } from 'fuels';
import { AMM_CONTRACT_ADDRESS, BASE_ASSET_ID } from './const.js';
import { AssetIdInput } from './types/fuel/Amm.js';
import crypto from 'crypto';

export const normalizeTxDate = (date: Date) => {
  const txDate = new Date(date);
  // Remove minutes, seconds and milliseconds from date
  txDate.setMinutes(0, 0, 0);
  return txDate;
};

export const findSenderFromInputs = (inputs: Array<Input>) => {
  for (const input of inputs) {
    if (input.type == InputType.Coin && input.assetId == BASE_ASSET_ID) {
      return input.owner;
    }
    if (input.type == InputType.Message && !(input.amount?.isZero())) {
      return input.recipient;
    }
  }
  return undefined;
}

export type PoolId = [AssetIdInput, AssetIdInput, boolean];

export const poolIdToStr = (poolId: PoolId) => `${poolId[0].bits.slice(2)}-${poolId[1].bits.slice(2)}-${poolId[2]}`;

export function getLPAssetId(poolId: PoolId) {
  const contractBuffer = Buffer.from(AMM_CONTRACT_ADDRESS.slice(2), 'hex');
  const subId = crypto.createHash('sha256')
    .update(Buffer.from(poolId[0].bits.slice(2), 'hex'))
    .update(Buffer.from(poolId[1].bits.slice(2), 'hex'))
    .update(Buffer.from(poolId[2] ? '01' : '00', 'hex'))
    .digest();

  return '0x' + crypto.createHash('sha256').update(contractBuffer).update(subId).digest('hex');
}
