import assert from 'assert'
import { before, describe, test } from 'node:test'
import { TestProcessorServer } from '@sentio/sdk/testing'
import {NETWORK_ID} from "./const.js";
import fs from 'fs';
import { Campaign, Position } from "./schema/store.js";

const test_data: JSON[] = [];
const test_data_dir = "./src/test-data";
fs.readdirSync(test_data_dir).forEach((file) => {
  fs.readFile(`${test_data_dir}/${file}`, 'utf8', (err, data) => {
    if (err) {
      console.error('Error reading file:', err);
      return;
    }
    test_data.push(JSON.parse(data));
  });
});


describe('Test Processor', () => {
  const service = new TestProcessorServer(() => import('./campaignsProcessor.js'))

  before(async () => {
    await service.start()
  })

  test('has valid config', async () => {
    const config = await service.getConfig({})
    assert(config.contractConfigs.length > 0)
    assert.equal(config.contractConfigs[0].fuelReceiptConfigs.length, 7)
  })

  test('test onLog ', async () => {
    for (const testData of test_data) {
      const res = await service.fuel.testOnTransaction(testData, NETWORK_ID);
      // console.log(JSON.stringify(res))
    }
    assert.equal((await service.store.list(Campaign)).length, 0)
    assert.equal((await service.store.list(Position)).length, 0)
  });

})
