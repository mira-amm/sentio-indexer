// import { Counter } from '@sentio/sdk'
// import { ERC20Processor } from '@sentio/sdk/eth/builtin'

// const tokenCounter = Counter.register('token')

// const address = '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'

// ERC20Processor.bind({ address }).onEventTransfer(async (event, ctx) => {
//   const val = event.args.value.scaleDown(18)
//   tokenCounter.add(ctx, val)
// })


import { LogLevel } from '@sentio/sdk'
import { FuelNetwork } from '@sentio/sdk/fuel'
import { AmmProcessor } from './types/fuel/AmmProcessor.js'

const contractAddress = '0xd5a716d967a9137222219657d7877bd8c79c64e1edb5de9f2901c98ebe74da80';

AmmProcessor.bind({
  address: contractAddress,
  chainId: FuelNetwork.TEST_NET
}).onLogCreatePoolEvent(async (event, ctx) => {
  console.log(event);
  ctx.meter.Counter('pools').add(1);
  ctx.eventLogger.emit("PairCreated", {
    token0: event.data.pool_id[0],
    token1: event.data.pool_id[0],
    stable: event.data.pool_id[2],
  });
});

// // import { CounterContractProcessor } from './types/fuel/CounterContractProcessor.js'

// CounterContractProcessor.bind({
//       address: '0xa14f85860d6ce99154ecbb13570ba5fba1d8dc16b290de13f036b016fd19a29c',
//       chainId: FuelNetwork.TEST_NET
//     })
//     .onTransaction(
//         async (tx, ctx) => {
//           ctx.eventLogger.emit('transaction', {
//             distinctId: tx.id,
//             message: 'Transaction processed',
//             properties: {
//               fee: tx.fee.toNumber()
//             },
//             severity: tx.status === 'success' ? LogLevel.INFO : LogLevel.ERROR
//           })
//         },
//         { includeFailed: true }
//     )
//     .onLogFoo(async (log, ctx) => {
//       ctx.meter.Counter('fooLogged').add(1, { baz: String(log.data.baz) })
//     })
//     .onTimeInterval(async (block, ctx) => {
//       ctx.eventLogger.emit('block', {
//         ...block,
//       })
//       ctx.meter.Counter('interval').add(1)
//     }, 60 * 24)
