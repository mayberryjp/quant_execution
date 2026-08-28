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

    @property
    def exit_side(self) -> OrderSide:
        """Exit order side (opposite of entry): SELL to close a LONG, BUY to cover a SHORT."""
        return OrderSide.SELL if self is PositionType.LONG else OrderSide.BUY


class ExitReason(StrEnum):
    TARGET_PRICE = "TARGET_PRICE"
    MARKET_CLOSE = "MARKET_CLOSE"


class TradeStatus(StrEnum):
    NEW = "NEW"
    CASH_HELD = "CASH_HELD"
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    INSUFFICIENT_FUNDS = "INSUFFICIENT_FUNDS"
    FAILED = "FAILED"
    # Exit lifecycle (the entry row is updated in place; SPEC.md §5.7).
    EXIT_SUBMITTED = "EXIT_SUBMITTED"
    EXIT_PARTIALLY_FILLED = "EXIT_PARTIALLY_FILLED"
    CLOSED = "CLOSED"
    EXIT_FAILED = "EXIT_FAILED"
