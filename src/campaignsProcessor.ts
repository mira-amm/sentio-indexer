/*
 *
 */
import { FuelContractContext } from "@sentio/sdk/fuel";
import { AMM_CONTRACT_ADDRESS, NETWORK_ID } from "./const.js";
import { Campaign, Position } from "./schema/store.js";
import { MiraFarmerProcessor } from "./types/fuel/MiraFarmerProcessor.js";
import { MiraFarmer } from "./types/fuel/MiraFarmer.js";

const campaignAcrueRewards = async (
  campaign: Campaign,
  ctx: FuelContractContext<MiraFarmer>
) => {
};


const processor = MiraFarmerProcessor.bind({
  address: AMM_CONTRACT_ADDRESS,
  chainId: NETWORK_ID,
});

processor.onLogNewCampaignEvent(async (event, ctx) => {
  if (ctx.transaction?.status === "success") {
    ctx.meter.Counter("campaigns").add(1);
    ctx.eventLogger.emit("CampaignCreated", {
      id: event.data.campaign_id,
    });
    const campaign = new Campaign({
      id: event.data.campaign_id.toString(),
      lastAccrualTime: 0,
      stakingToken: event.data.staking_token.bits,
      startTime: event.data.start_time.toNumber(),
      endTime: event.data.end_time.toNumber(),
      rewardAssetId: event.data.reward_asset.bits,
      rewardsAccruedPerStakingToken: 0,
      totalRemainingRewards: 0,
      // rewardRate: event.data.reward_rate.toNumber(),
      owner: event.data.owner.Address?.bits || event.data.owner.ContractId?.bits || "",
    });
    await ctx.store.upsert(campaign);
  }
});

processor.onLogNewPositionEvent(async (event, ctx) => {
  if (ctx.transaction?.status === "success") {
    ctx.meter.Counter("positions").add(1);
    ctx.eventLogger.emit("PositionCreated", {
      id: event.data.position_id,
    });
    const position = new Position({
      id: event.data.position_id.toString(),
      identity: event.data.owner.Address?.bits ||
        event.data.owner.ContractId?.bits || "",
      stakingTokens: 0,
      lastAccrualTime: 0,
      rewardAssetId: event.data.asset_id.bits,
      rewardsAccrued: 0,
      // pendingRewardsTotal: 0,
    });
    await ctx.store.upsert(position);

    
    // how know what campaign????
    // campaignAcrueRewards


  }
});

// deposit_assets
processor.onLogPositionDepositEvent(async (event, ctx) => {
  if (ctx.transaction?.status === "success") {
    const positionId = event.data.position_id;
    const amount = event.data.amount.toNumber();

    ctx.eventLogger.emit("PositionDeposit", {
      positionId: positionId,
      amount: amount,
    });
    // We do not create the position if it does not already exist since the smart contract should revert in this case
    // Should revert if the asset is not the staking asset...
    let position = await ctx.store.get(Position, event.data.position_id.toString());
    if(position) {
      position.stakingTokens += amount;
      await ctx.store.upsert(position);
    }
    // event.data.asset
    // event.data.amount
  }
});

// // withdraw_assets
// processor.onLogPositionWithdrawEvent(async (event, ctx) => {
//     if (ctx.transaction?.status === "success") {
//         ctx.eventLogger.emit("PositionWithdraw", {
//             positionId: event.data.position_id,
//             amount: event.data.amount.toNumber(),
//         });
//         // We do not create the position if it does not already exist since the smart contract should revert in this case
//         // Should revert if the asset is not the staking asset...
//         let position = await ctx.store.get(Position, event.data.position_id.toString());
//         // event.data.asset
//         // event.data.amount
//     }
// });

// // claim_rewards
// processor.onLogClaimRewardsEvent(async (event, ctx) => {
//     if (ctx.transaction?.status === "success") {
//         ctx.eventLogger.emit("ClaimRewards", {
//             positionId: event.data.position_id,
//             amount: event.data.amount.toNumber(),
//         });
//         // We do not create the position if it does not already exist since the smart contract should revert in this case
//         // Should revert if the asset is not the staking asset...
//         let position = await ctx.store.get(Position, event.data.position_id.toString());
//         // event.data.asset
//         // event.data.amount
//     }
// });

// fund_rewards
processor.onLogCampaignFundedEvent(async (event, ctx) => {
  if (ctx.transaction?.status === "success") {
    ctx.eventLogger.emit("CampaignFunded", {
      campaignId: event.data.campaign_id,
      amount: event.data.amount.toNumber(),
    });

    let campaign = await ctx.store.get(Campaign, event.data.campaign_id.toString());
    if (!campaign) {
      // log("Campaign not found", event.data.campaign_id.toString());
      return;
    }
    campaign.totalRemainingRewards += event.data.amount.toNumber();
    
    campaignAcrueRewards(campaign, ctx);
    
  }
});

processor.onLogCampaignExtendedEvent(async (event, ctx) => {
  if (ctx.transaction?.status === "success") {
    ctx.eventLogger.emit("CampaignExtended", {
      campaignId: event.data.campaign_id,
      endTime: event.data.new_end_time.toNumber(),
    });
    // The new rate just be included in the event raised by the contract
  }
});


processor.onLogCampaignExitedEvent(async (event, ctx) => {

});

// join_campaign
processor.onLogCampaignJoinedEvent(async (event, ctx) => {

});
