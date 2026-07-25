"""Order price helpers and CLOB order-type utilities."""

from __future__ import annotations

from typing import cast

from py_clob_client_v2.clob_types import OrderType


def clamp_buy_price(price: float, slippage: float = 0.01) -> float:
    return max(0.01, min(0.99, round(price + slippage, 2)))


def clamp_sell_price(price: float, slippage: float = 0.01) -> float:
    return max(0.01, min(0.99, round(price - slippage, 2)))


def parse_order_type(name: str) -> OrderType:
    key = (name or "GTC").upper()
    if key == "FOK":
        return cast(OrderType, OrderType.FOK)
    return cast(OrderType, OrderType.GTC)
