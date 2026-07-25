"""Momentum — buy YES or NO when its bid is at/above the entry threshold."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from strategies.base import MomentumDecision
from strategies.momentum_execution import execute_momentum_decision
from utils.momentum_risk import is_market_locked, pick_momentum_side

if TYPE_CHECKING:
    from bot import MarketWorker


class MomentumStrategy:
    async def evaluate(self, worker: "MarketWorker") -> Optional[MomentumDecision]:
        from bot import OrderState, is_locked_price

        if worker.order_state == OrderState.PENDING:
            return None

        cfg = worker.worker_config
        yes_bid = worker.best_bid("YES")
        no_bid = worker.best_bid("NO")

        if is_market_locked(yes_bid, no_bid, is_locked=is_locked_price):
            worker.log_locked_market_skip(yes_bid, no_bid)
            return None

        picked = pick_momentum_side(
            yes_bid,
            no_bid,
            cfg.momentum_entry_threshold,
            is_locked=is_locked_price,
        )
        if picked is None:
            return None

        side, bid = picked

        if not worker.can_enter():
            return None

        size = worker.entry_order_size([side])
        if size is None:
            return None

        return MomentumDecision(
            side=side,
            price=round(bid, 2),
            size=size,
            trigger_price=round(bid, 4),
        )

    async def execute(self, worker: "MarketWorker", decision: MomentumDecision) -> None:
        await execute_momentum_decision(worker, decision)
