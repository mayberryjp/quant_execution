"""In-memory position book — the hot-path source of truth for open positions (SPEC.md §5.7).

Every open position (an entry order that filled, or is awaiting fill in live mode) is tracked here
so the per-tick exit check is a pure in-memory O(1) lookup by symbol, with no database read on the
streaming path. Durable state is written asynchronously (see :mod:`quant_execution.repository`),
and the book is rehydrated from open trades at startup.

The book is guarded by a single lock. Claims (``claim_exits`` / ``claim_all_open``) atomically flip
a position to ``EXITING`` so an exit is submitted exactly once even if the market-close liquidator
and a tick fire at the same instant.
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum

from quant_execution.domain.enums import OrderSide, PositionType

__all__ = ["Position", "PositionBook", "PositionState"]


class PositionState(StrEnum):
    PENDING_ENTRY = "PENDING_ENTRY"  # live entry submitted, awaiting fill confirmation
    OPEN = "OPEN"  # entry filled, exit-eligible
    EXITING = "EXITING"  # exit order submitted, awaiting close


@dataclass
class Position:
    """Mutable in-memory record of one position keyed by its trade id."""

    trade_id: uuid.UUID
    symbol: str
    position_type: PositionType
    quantity: Decimal
    entry_price: Decimal
    sell_price: Decimal | None
    is_paper: bool
    idempotency_key: str
    broker_order_id: str | None = None
    exit_broker_order_id: str | None = None
    cash_hold_id: uuid.UUID | None = None
    state: PositionState = field(default=PositionState.PENDING_ENTRY)

    @property
    def exit_side(self) -> OrderSide:
        return self.position_type.exit_side

    def exit_triggered(self, price: Decimal) -> bool:
        """Take-profit direction: LONG closes at/above, SHORT closes at/below ``sell_price``."""
        if self.sell_price is None:
            return False
        if self.position_type is PositionType.LONG:
            return price >= self.sell_price
        return price <= self.sell_price


class PositionBook:
    """Thread-safe set of open positions, indexed by symbol for O(1) matching."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._by_symbol: dict[str, dict[uuid.UUID, Position]] = {}

    def __len__(self) -> int:
        with self._lock:
            return sum(len(v) for v in self._by_symbol.values())

    @property
    def symbols(self) -> set[str]:
        with self._lock:
            return set(self._by_symbol)

    def open(self, position: Position) -> None:
        """Add (or replace) a position."""
        with self._lock:
            self._by_symbol.setdefault(position.symbol, {})[position.trade_id] = position

    def remove(self, position: Position) -> None:
        with self._lock:
            bucket = self._by_symbol.get(position.symbol)
            if bucket is not None:
                bucket.pop(position.trade_id, None)
                if not bucket:
                    del self._by_symbol[position.symbol]

    def mark_open(self, position: Position) -> None:
        with self._lock:
            position.state = PositionState.OPEN

    def claim_exits(self, symbol: str, price: Decimal) -> list[Position]:
        """Atomically flip OPEN positions on ``symbol`` whose target is hit to EXITING."""
        claimed: list[Position] = []
        with self._lock:
            bucket = self._by_symbol.get(symbol)
            if not bucket:
                return claimed
            for position in bucket.values():
                if position.state is PositionState.OPEN and position.exit_triggered(price):
                    position.state = PositionState.EXITING
                    claimed.append(position)
        return claimed

    def claim_all_open(self) -> list[Position]:
        """Atomically flip every OPEN position to EXITING (market-close liquidation)."""
        claimed: list[Position] = []
        with self._lock:
            for bucket in self._by_symbol.values():
                for position in bucket.values():
                    if position.state is PositionState.OPEN:
                        position.state = PositionState.EXITING
                        claimed.append(position)
        return claimed

    def release_exit(self, position: Position) -> None:
        """Return a position to OPEN after a failed exit so it can be retried."""
        with self._lock:
            position.state = PositionState.OPEN
            position.exit_broker_order_id = None

    def pending_and_exiting(self) -> list[Position]:
        """Snapshot of positions the live reconciler must poll the broker for."""
        with self._lock:
            return [
                position
                for bucket in self._by_symbol.values()
                for position in bucket.values()
                if position.state is not PositionState.OPEN
            ]
