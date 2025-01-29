import { before, after, describe, test } from 'node:test'
import { TestProcessorServer } from '@sentio/sdk/testing'
import {
  NETWORK_ID,
} from "./const.js";
import { State, StoreContext } from '@sentio/runtime'
import '@sentio/protos'
import fs from 'node:fs';
import { Pool, PoolSnapshot } from "./schema/store.js";
import assert from 'node:assert';

describe('Test Processor', () => {
  const service = new TestProcessorServer(async () => await import('./processor.js'))

  before(async () => {
    await service.start()
  })

  test('test onLog ', async () => {

  const filePath = 'src/test-data.json';

  const data = fs.readFileSync(filePath, 'utf8');

  const lines = data.split('\n');
  const jsonObjects = lines
    .map(line => {
      return JSON.parse(line)
    })
  for (let i = 0; i < jsonObjects.length; i++) {
    await service.fuel.testOnTransaction(jsonObjects[i], NETWORK_ID);
  }

    const pools = await service.store.list(Pool);
    const pool = pools.find(p => p.id === 'a0265fb5c32f6e8db3197af3c7eb05c48ae373605b8165b6f4a51c5b0ba4812e-d6acf12b095570eb604cd049bb3caf19e7100fa958ea7c981a9c06a019dff369-false');

    assert(pool, 'pool not found');
    assert(pool.reserve0 === 52348n, 'reserve0 not equal');
    assert(pool.reserve1 === 9793151397774n, 'reserve1 not equal');
});

  after(async () => {
    State.reset()
  })
})