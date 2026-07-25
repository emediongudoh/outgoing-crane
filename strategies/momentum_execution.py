"""GTC entry placement and fill monitoring for momentum trades."""

from __future__ import annotations

import asyncio
import random
import time
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from strategies.base import MomentumDecision
from utils.momentum_risk import is_order_fully_filled

if TYPE_CHECKING:
    from bot import MarketWorker


async def _simulate_dry_leg(
    worker: "MarketWorker",
    side: str,
    price: float,
    size: float,
) -> Tuple[str, float, float]:
    cfg = worker.worker_config
    delay_ms = random.randint(cfg.dry_run_fill_delay_min_ms, cfg.dry_run_fill_delay_max_ms)
    await asyncio.sleep(delay_ms / 1000.0)
    return side, size, price


async def _monitor_dry_fills(
    worker: "MarketWorker",
    decision: MomentumDecision,
    legs: List[Tuple[str, float]],
) -> Dict[str, Tuple[float, float]]:
    from bot import MIN_FILL_DELTA

    cfg = worker.worker_config
    order_size = float(decision.size)
    timeout_sec = cfg.fill_timeout_ms / 1000.0
    poll_sec = cfg.fill_poll_ms / 1000.0

    leg_tasks = {
        side: asyncio.create_task(_simulate_dry_leg(worker, side, price, order_size))
        for side, price in legs
    }
    fills: Dict[str, Tuple[float, float]] = {}
    started = time.monotonic()

    while leg_tasks and (time.monotonic() - started) < timeout_sec:
        for side, task in list(leg_tasks.items()):
            if not task.done():
                continue
            try:
                s, size, price = task.result()
            except asyncio.CancelledError:
                del leg_tasks[side]
                continue
            if size > MIN_FILL_DELTA:
                fills[s] = (size, price)
            del leg_tasks[side]
        if not leg_tasks:
            break
        await asyncio.sleep(poll_sec)

    for side, task in leg_tasks.items():
        task.cancel()
        print(f"  🧪 [DRY CANCEL] {side} timed out after {timeout_sec:.1f}s")

    return fills


async def _monitor_live_fills(
    worker: "MarketWorker",
    decision: MomentumDecision,
    legs: List[Tuple[str, float]],
    placed: List[Tuple[Optional[str], float]],
) -> Dict[str, Tuple[float, float]]:
    from bot import MIN_FILL_DELTA

    cfg = worker.worker_config
    order_size = float(decision.size)
    timeout_sec = cfg.fill_timeout_ms / 1000.0
    poll_sec = cfg.fill_poll_ms / 1000.0

    fills: Dict[str, Tuple[float, float]] = {}
    pending: Dict[str, Tuple[str, float, float]] = {}

    for (side, limit_price), (order_id, immediate_fill) in zip(legs, placed):
        if not order_id:
            continue
        if immediate_fill:
            fills[side] = (order_size, limit_price)
            worker._untrack_order(order_id)
        else:
            pending[side] = (order_id, limit_price, order_size)

    started = time.monotonic()
    while pending and (time.monotonic() - started) < timeout_sec:
        for side in list(pending.keys()):
            order_id, limit_price, requested = pending[side]
            fill_size, fill_price = await worker.poll_order_fill(
                order_id, requested, limit_price,
            )
            if fill_size > MIN_FILL_DELTA:
                existing = fills.get(side, (0.0, 0.0))
                if existing[0] > MIN_FILL_DELTA:
                    total = existing[0] + fill_size
                    avg_px = (existing[0] * existing[1] + fill_size * fill_price) / total
                    fills[side] = (total, avg_px)
                else:
                    fills[side] = (fill_size, fill_price)
            if is_order_fully_filled(requested, fill_size, MIN_FILL_DELTA):
                worker._untrack_order(order_id)
                del pending[side]
        if not pending:
            break
        await asyncio.sleep(poll_sec)

    for _side, (order_id, limit_price, requested) in list(pending.items()):
        fill_size, fill_price = await worker.cancel_order_confirmed(
            order_id, requested, limit_price,
        )
        if fill_size > MIN_FILL_DELTA:
            existing = fills.get(_side, (0.0, 0.0))
            if existing[0] > MIN_FILL_DELTA:
                total = existing[0] + fill_size
                avg_px = (existing[0] * existing[1] + fill_size * fill_price) / total
                fills[_side] = (total, avg_px)
            else:
                fills[_side] = (fill_size, fill_price)

    return fills


def _record_fills(
    worker: "MarketWorker",
    fills: Dict[str, Tuple[float, float]],
) -> None:
    from bot import MIN_FILL_DELTA

    for side, (size, price) in fills.items():
        if size > MIN_FILL_DELTA and price > 0:
            worker.inventory.record_buy(side, size, price)
            print(f"  ✅ [FILL] {side} {size:.2f}@{round(price*100)}c")


async def execute_momentum_decision(worker: "MarketWorker", decision: MomentumDecision) -> None:
    from bot import OrderState

    cfg = worker.worker_config
    legs = worker.resolve_execution_legs(decision)
    if not legs:
        print(
            f"❌ [MOMENTUM ABORT] {worker.asset_type.upper()} {worker.window_slug} | "
            f"no executable leg (missing/locked bid)"
        )
        return

    if not worker.validate_entry_execution(decision, legs):
        return

    for side, _price in legs:
        if not worker.validate_order_size(side, decision.size):
            print(
                f"❌ [MOMENTUM ABORT] {worker.asset_type.upper()} {worker.window_slug} | "
                f"{side} size={decision.size} failed pre-submit sanity check"
            )
            return

    worker.order_state = OrderState.PENDING
    try:
        leg_str = " ".join(f"{s}@{round(p*100)}c" for s, p in legs)
        if worker.is_dry_run():
            print(
                f"\n🧪 [DRY MOMENTUM] {worker.asset_type.upper()} {worker.window_slug} | "
                f"{decision.side} trigger={decision.trigger_price:.4f} size={decision.size} | "
                f"{leg_str} | monitor {cfg.fill_timeout_ms}ms max"
            )
            start = time.monotonic()
            fills = await _monitor_dry_fills(worker, decision, legs)
            _record_fills(worker, fills)
            if fills:
                worker.log_entry_trades(fills=fills)
            worker._log_entry(decision, fills=fills or None, dry_run=True)
            print(f"  🧪 [DRY MOMENTUM] cycle done in {time.monotonic() - start:.2f}s")
            return

        print(
            f"\n📊 [MOMENTUM] {worker.asset_type.upper()} {worker.window_slug} | "
            f"{decision.side} trigger={decision.trigger_price:.4f} | {leg_str}"
        )

        order_size = float(decision.size)
        placed = await asyncio.gather(
            *[worker.place_entry_gtc(side, price, order_size) for side, price in legs]
        )

        fills = await _monitor_live_fills(worker, decision, legs, placed)
        _record_fills(worker, fills)

        if fills:
            worker.log_entry_trades(fills=fills)
        worker._log_entry(decision, fills=fills or None)
    finally:
        worker.order_state = OrderState.IDLE
