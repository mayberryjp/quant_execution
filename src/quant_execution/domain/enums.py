"""Domain enumerations shared across the service.

String-valued enums so the values persist directly as the ``VARCHAR`` column values defined in
the ``trades`` schema (SPEC.md §3.2 / §3.3).
"""

from __future__ import annotations

from enum import StrEnum


class ExecutionMode(StrEnum):
    PAPER = "paper"
    LIVE = "live"

    @property
    def is_paper(self) -> bool:
        return self is ExecutionMode.PAPER


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class PositionType(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"

    @property
    def entry_side(self) -> OrderSide:
        """Entry order side derived from the position type (SPEC.md §3.2)."""
        return OrderSide.BUY if self is PositionType.LONG else OrderSide.SELL


class TradeStatus(StrEnum):
    NEW = "NEW"
    CASH_HELD = "CASH_HELD"
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    INSUFFICIENT_FUNDS = "INSUFFICIENT_FUNDS"
    FAILED = "FAILED"
