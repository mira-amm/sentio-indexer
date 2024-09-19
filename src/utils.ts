import { InputType, Input } from 'fuels';
import { BASE_ASSET_ID } from './const.js';

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

