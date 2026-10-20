"""GUARD_COOLDOWN_S: no maker (re-)post in a slot for N s after a maker guard block/cancel; pre-post twap_cross."""
import time
from unittest.mock import AsyncMock, MagicMock

import config
import maker_guard
from test_maker_guard import TestMakerGuardBotIntegration as _Base


class TestGuardCooldown(_Base):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self._cd = config.GUARD_COOLDOWN_S
        config.GUARD_COOLDOWN_S = 15.0

    async def asyncTearDown(self):
        config.GUARD_COOLDOWN_S = self._cd
        await super().asyncTearDown()

    def _sig(self):
        return MagicMock(side="UP", target_bid=0.44, edge_net=0.05, maker_rebate=0.0)

    async def test_twap_cross_cancel_starts_cooldown(self):
        t0 = time.time()
        slot = await self._run(spot=84340.0, price_to_beat=84360.0)
        self.assertIsNone(slot.resting_order)
        self.assertGreaterEqual(slot.guard_cooldown_until, t0 + 14.9)

    async def test_cooldown_blocks_repost(self):
        slot = await self._run(spot=84340.0, price_to_beat=84360.0)
        self.bot.risk.calculate_stake = MagicMock(side_effect=AssertionError("must not size a post"))
        await self.bot._place_maker_bid(slot, slot.market, self._sig())
        self.assertIsNone(slot.resting_order)
        self.assertIn("quote:guard_cooldown", slot.maker_block_logged)

    async def test_cooldown_expires(self):
        slot = self.bot.slots["btc"]
        slot.market = {"slug": "s", "tokens": {"UP": "tok_up"}}
        slot.guard_cooldown_until = time.time() - 0.1
        self.bot.risk.calculate_stake = MagicMock(return_value=0.0)  # passes the gate, then skips on stake
        await self.bot._place_maker_bid(slot, slot.market, self._sig())
        self.bot.risk.calculate_stake.assert_called_once()
        self.assertNotIn("quote:guard_cooldown", slot.maker_block_logged)

    async def test_cooldown_off(self):
        config.GUARD_COOLDOWN_S = 0.0
        slot = await self._run(spot=84340.0, price_to_beat=84360.0)
        self.assertEqual(slot.guard_cooldown_until, 0.0)

    async def test_only_guard_reasons_start_cooldown(self):
        slot = self.bot.slots["btc"]
        for r in ("no_twap_strike", "post_falling_mid", "guard_cooldown", None):
            self.bot._start_guard_cooldown(slot, r)
        self.assertEqual(slot.guard_cooldown_until, 0.0)
        for r in maker_guard.COOLDOWN_REASONS:
            slot.guard_cooldown_until = 0.0
            self.bot._start_guard_cooldown(slot, r)
            self.assertGreater(slot.guard_cooldown_until, time.time() + 14.0)

    async def test_pre_post_twap_cross_blocks_quote(self):
        # z checks pass (e.g. realized TWAP still favourable) but spot is already across the TWAP strike.
        orig = maker_guard.check_quote
        maker_guard.check_quote = lambda side, zs, zt, m: (True, None, 1.0, "spot")
        self.addCleanup(setattr, maker_guard, "check_quote", orig)
        slot = await self._run(spot=84340.0, price_to_beat=84360.0, resting=False)
        self.assertIn("quote:twap_cross", slot.maker_block_logged)
        self.assertEqual(self._maker_consults(), [])
        self.assertIsNone(slot.resting_order)
        self.assertGreater(slot.guard_cooldown_until, time.time())
