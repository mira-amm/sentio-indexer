/*
 *
 */
import { AMM_CONTRACT_ADDRESS, NETWORK_ID } from "./const.js";
import { Campaign, Position } from "./schema/store.js";
import { AmmProcessor } from "./types/fuel/AmmProcessor.js";
import { MiraFarmerProcessor } from "./types/fuel/MiraFarmerProcessor.js";


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
            stakingToken: event.data.staking_token.bits,
            startTime: event.data.start_time.toNumber(),
            endTime: event.data.end_time.toNumber(),
            rewardAssetId: event.data.reward_asset.bits,
            rewardRate: event.data.reward_rate.toNumber(),
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
            rewardAssetId: event.data.asset_id.bits,
            identity: event.data.owner.Address?.bits ||
                event.data.owner.ContractId?.bits || "",
            pendingRewardsTotal: 0,
        });
        await ctx.store.upsert(position);
    }
});

// deposit_assets
processor.onLogPositionDepositEvent(async (event, ctx) => {
    if (ctx.transaction?.status === "success") {
        ctx.eventLogger.emit("PositionDeposit", {
            positionId: event.data.position_id,
            amount: event.data.amount.toNumber(),
        });
        // We do not create the position if it does not already exist since the smart contract should revert in this case
        // Should revert if the asset is not the staking asset...
        let position = await ctx.store.get(Position, event.data.position_id.toString());
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
        campaign.totalPendingRewards += event.data.amount.toNumber();

        // log(CampaignFundedEvent {
        //     campaign_id,
        //     amount,
        //     new_reward_rate: campaign.reward_rate,
        // });

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
