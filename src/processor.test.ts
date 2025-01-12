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
import {DBRequest_DBList} from '@sentio/protos'
import { DBRequest } from '@sentio/protos';
import { CallContext } from 'nice-grpc-common'
import { Pool } from "./schema/store.js";
import { AsyncLocalStorage } from 'node:async_hooks'
import {BaseContext} from '@sentio/sdk/core'

// const TEST_CONTEXT: FuelContext = new FuelContext(NETWORK_ID, AMM_CONTRACT_ADDRESS, "Amm", new Date(), null, null)

// const TEST_CONTEXT: BaseContext = new BaseContext()

const dbContextLocalStorage = new AsyncLocalStorage<StoreContext | undefined>()

describe('Test Processor', () => {
  const service = new TestProcessorServer(async () => await import('./processor.js'))

  // const service = new TestProcessorServer(async () => {
  //   AmmProcessor.bind({
  //   // FuelGlobalProcessor.bind({
  //     address: AMM_CONTRACT_ADDRESS,
  //     chainId: NETWORK_ID,
  //   }).
  //     onLogCreatePoolEvent(async (tx, ctx) => {
  //     ctx.eventLogger.emit('tx', {
  //       distinctId: "blah",//`${ctx.transaction?.id}_${ctx.transaction?.blockId}`,
  //       message: `tx  ${tx}`,
  //       attributes: {}
  //     });
  //   });

  // });

  before(async () => {

    // @ts-ignore
    // await service.start(undefined, TEST_CONTEXT)


    await service.start()

    // TEST_CONTEXT.initStore()
    // dbContextLocalStorage.run(undefined, () => (TEST_CONTEXT.initStore()))
  })

  test('has valid config', async () => {
    const config = await service.getConfig({})
      assert(config.contractConfigs.length > 0)
      // CreatePoolEvent, SwapEvent, MintEvent, BurnEvent
    assert.equal(config.contractConfigs[0].fuelLogConfigs.length, 4)
    // assert(false, JSON.stringify(service.service.getConfig()));

  })
  

  test('test onLog ', async () => {
    const res = await service.fuel.testOnTransaction(testData, NETWORK_ID);
    console.log(JSON.stringify(res))
    // console.log(TEST_CONTEXT.store.list(Pool).toString())
    // console.log(    State.toString())
    // assert(false, JSON.stringify(State));
    // assert(false, JSON.stringify(getStore()));

    // assert(JSON.stringify(service.contractConfigs))
    // const events = res.result?.events;
    // assert.equal(events?.length, 2)
    // assert.equal(events?.[1]?.message, 'log foo');
  });

  after(async () => {
    State.reset()
  })

})
