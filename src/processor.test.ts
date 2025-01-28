import { FuelContext, FuelGlobalProcessor, FuelAbstractProcessor } from "@sentio/sdk/fuel";
import assert from 'assert'
import { before, after, describe, test } from 'node:test'
import { TestProcessorServer } from '@sentio/sdk/testing'
import { getStore } from '@sentio/sdk/store'
import {
  AMM_CONTRACT_ADDRESS,
  BASE_ASSET_ID,
  NETWORK_ID,
  NETWORK_NAME,
} from "./const.js";
import testData from './test-data.json' assert {type: 'json'};
import { State, StoreContext } from '@sentio/runtime'
import '@sentio/protos'
// import { Pool, PoolSnapshot } from "./schema/store.js";
import fs from 'node:fs';
import { Pool, PoolSnapshot } from "./schema/store.js";

describe('Test Processor', () => {
  const service = new TestProcessorServer(async () => await import('./processor.js'))

  before(async () => {
    await service.start()
  })

  // test('has valid config', async () => {
  //   const config = await service.getConfig({})
  //     assert(config.contractConfigs.length > 0)
  //     // CreatePoolEvent, SwapEvent, MintEvent, BurnEvent
  //   assert.equal(config.contractConfigs[0].fuelLogConfigs.length, 4)
  //   // assert(false, JSON.stringify(service.service.getConfig()));
  // })
  test('test onLog ', async () => {

  const filePath = '/Users/cmaklin/sentio-indexer/src/final.json';

  const data = fs.readFileSync(filePath, 'utf8');

  const lines = data.split('\n');
  const jsonObjects = lines
    .map(line => {
      return JSON.parse(line)
    })
  let i = 0;
  for (let i = 0; i < jsonObjects.length; i++) {
    console.log("transactionId:" + jsonObjects[i].id)
    const res = await service.fuel.testOnTransaction(jsonObjects[i], NETWORK_ID);
  }

    const pools = await service.store.list(Pool);
    const pool = pools.find(p => p.id === 'a0265fb5c32f6e8db3197af3c7eb05c48ae373605b8165b6f4a51c5b0ba4812e-d6acf12b095570eb604cd049bb3caf19e7100fa958ea7c981a9c06a019dff369-false');
    if (pool) {
      console.log({
        "reserve0": pool.reserve0,
        "reserve1": pool.reserve1
      });
    } else {
      console.log("pool not found");
    }
});

  after(async () => {
    State.reset()
  })
})