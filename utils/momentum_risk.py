"""Pure helpers for momentum entry and stop-loss validation."""

from __future__ import annotations

from typing import Callable, Optional


def side_meets_entry_threshold(price: float, threshold: float) -> bool:
    """Return True when a side's bid meets the momentum entry floor."""
    return price >= threshold


def stop_loss_triggered(current: float, entry: float, stop_loss_pct: float) -> bool:
    """Return True when current price has fallen stop_loss_pct below entry."""
    if entry <= 0 or current <= 0:
        return False
    return current <= entry * (1.0 - stop_loss_pct)


def stop_loss_price(entry: float, stop_loss_pct: float) -> float:
    return round(entry * (1.0 - stop_loss_pct), 4)


def is_market_locked(
    yes_price: float,
    no_price: float,
    *,
    is_locked: Callable[[float], bool],
) -> bool:
    """Skip markets where either side is at a resolved/locked price."""
    if yes_price > 0 and is_locked(yes_price):
        return True
    if no_price > 0 and is_locked(no_price):
        return True
    return False


def pick_momentum_side(
    yes_bid: float,
    no_bid: float,
    threshold: float,
    *,
    is_locked: Callable[[float], bool],
) -> Optional[tuple[str, float]]:
    """Return the side with the highest qualifying bid, or None."""
    candidates: list[tuple[str, float]] = []
    for side, bid in (("YES", yes_bid), ("NO", no_bid)):
        if bid <= 0 or is_locked(bid):
            continue
        if side_meets_entry_threshold(bid, threshold):
            candidates.append((side, bid))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[1])


def entry_window_ok(
    seconds_left: int,
    *,
    entry_seconds_left: int,
    min_entry_seconds_left: int,
) -> bool:
    """Entry allowed when within the configured time window."""
    return min_entry_seconds_left < seconds_left <= entry_seconds_left


def is_order_fully_filled(requested: float, filled: float, min_delta: float) -> bool:
    return filled >= requested - min_delta
