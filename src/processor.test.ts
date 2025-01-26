import assert from 'assert'
import { before, describe, test } from 'node:test'
import { TestProcessorServer } from '@sentio/sdk/testing'
import {
  AMM_CONTRACT_ADDRESS,
  BASE_ASSET_ID,
  NETWORK_ID,
  NETWORK_NAME,
} from "./const.js";
import testData from './test-data.json' assert {type: 'json'};
import { PoolSnapshot, Pool } from "./schema/store.js";

describe('Test Processor', () => {
  const service = new TestProcessorServer(() => import('./processor.js'))

  before(async () => {
    await service.start()
  })

  test('has valid config', async () => {
    const config = await service.getConfig({})
    assert(config.contractConfigs.length > 0)
    // CreatePoolEvent, SwapEvent, MintEvent, BurnEvent
    assert.equal(config.contractConfigs[0].fuelLogConfigs.length, 4)
  })
  

  test('test onLog ', async () => {
    const res = await service.fuel.testOnTransaction(testData, NETWORK_ID);
    // console.log(JSON.stringify(res))
    console.log((await service.store.list(Pool))[0])
    console.log(await service.store.list(PoolSnapshot))

  });

})
