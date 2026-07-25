"""Strategy types for momentum trading."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Protocol

if TYPE_CHECKING:
    from bot import MarketWorker


@dataclass(frozen=True)
class MomentumDecision:
    side: str
    price: float
    size: float
    trigger_price: float


class MomentumStrategyProtocol(Protocol):
    async def evaluate(self, worker: "MarketWorker") -> Optional[MomentumDecision]:
        ...

    async def execute(self, worker: "MarketWorker", decision: MomentumDecision) -> None:
        ...
