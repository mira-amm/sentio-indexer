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
  // TODO: rewards
};

const positionAcrueRewards = async (
  position: Position,
  ctx: FuelContractContext<MiraFarmer>
) => {
  // TODO: rewards
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
      stakingTokens: 0,
      startTime: event.data.start_time.toNumber(),
      endTime: event.data.end_time.toNumber(),
      rewardAssetId: event.data.reward_asset.bits,
      // TODO: rewards
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
      // Why do we care who owns this?
      // identity: event.data.owner.Address?.bits ||
      //   event.data.owner.ContractId?.bits || "",
      stakingToken: event.data.asset_id.bits,
      stakingTokens: 0,
      lastAccrualTime: 0,
      // TODO: Rewards
    });

    await ctx.store.upsert(position);

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
    if (!position) {
      // log("Position not found", event.data.position_id.toString());
      return;
    }
    // TODO: How get campaign??
    // campaignAcrueRewards(campaign, ctx);
    positionAcrueRewards(position, ctx)
    position.stakingTokens += amount;
    await ctx.store.upsert(position);

  }
});

// withdraw_assets
processor.onLogPositionWithdrawEvent(async (event, ctx) => {
  if (ctx.transaction?.status === "success") {
    const positionId = event.data.position_id;
    const amount = event.data.amount.toNumber();
    ctx.eventLogger.emit("PositionWithdraw", {
      positionId: positionId,
      amount: amount,
    });
    // We do not create the position if it does not already exist since the smart contract should revert in this case
    // Should revert if the asset is not the staking asset...
    let position = await ctx.store.get(Position, positionId.toString());
    if (!position) {
      // log("Position not found", event.data.position_id.toString());
      return;
    }
    // TODO: How get campaign??
    // campaignAcrueRewards(campaign, ctx);
    positionAcrueRewards(position, ctx);
    position.stakingTokens -= amount;
    await ctx.store.upsert(position);

  }
});

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
    campaignAcrueRewards(campaign, ctx);

    // TODO: Rewards
    // campaign.??? += event.data.amount.toNumber();
    
    await ctx.store.upsert(campaign);

  }
});

processor.onLogCampaignExtendedEvent(async (event, ctx) => {
  if (ctx.transaction?.status === "success") {
    const endTime = event.data.new_end_time.toNumber();
    ctx.eventLogger.emit("CampaignExtended", {
      campaignId: event.data.campaign_id,
      endTime: endTime,
    });
    let campaign = await ctx.store.get(Campaign, event.data.campaign_id.toString());
    if (!campaign) {
      // log("Campaign not found", event.data.campaign_id.toString());
      return;
    }

    campaignAcrueRewards(campaign, ctx);
    campaign.endTime = endTime;
    
    // TODO: Rewards

    await ctx.store.upsert(campaign);
  }
});


// processor.onLogCampaignExitedEvent(async (event, ctx) => {

// });

// join_campaign
processor.onLogCampaignJoinedEvent(async (event, ctx) => {
  if (ctx.transaction?.status === "success") {
    ctx.eventLogger.emit("JoinCampaign", {
      positionId: event.data.position_id,
      campaignId: event.data.campaign_id,
    });
    let position = await ctx.store.get(Position, event.data.position_id.toString());
    if (!position) {
      // log("Position not found", event.data.position_id.toString());
      return;
    }
    let campaign = await ctx.store.get(Campaign, event.data.campaign_id.toString());
    if (!campaign) {
      // log("Campaign not found", event.data.campaign_id.toString());
      return;
    }

    campaignAcrueRewards(campaign, ctx);
    positionAcrueRewards(position, ctx);

    // TODO: how get time?
    position.lastAccrualTime = 0;

    campaign.stakingTokens += event.data.amount.toNumber();

    await ctx.store.upsert(position);
    await ctx.store.upsert(campaign);
  }

});
