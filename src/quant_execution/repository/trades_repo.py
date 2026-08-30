"""Data-access helpers for the ``trades`` table.

All access goes through the SQLAlchemy ORM / Core, which parameterizes values; raw string SQL
interpolation is never used (SPEC.md §3.1, backend standards).
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from quant_execution.domain.enums import TradeStatus
from quant_execution.repository.models import Trade

__all__ = ["TradesRepository"]


class TradesRepository:
    """Repository over a single SQLAlchemy :class:`Session`."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def exists_by_idempotency_key(self, idempotency_key: str) -> bool:
        stmt = select(Trade.id).where(Trade.idempotency_key == idempotency_key).limit(1)
        return self._session.execute(stmt).first() is not None

    def get(self, trade_id: uuid.UUID) -> Trade | None:
        return self._session.get(Trade, trade_id)

    def get_by_idempotency_key(self, idempotency_key: str) -> Trade | None:
        stmt = select(Trade).where(Trade.idempotency_key == idempotency_key).limit(1)
        return self._session.execute(stmt).scalar_one_or_none()

    def insert(self, trade: Trade) -> Trade:
        self._session.add(trade)
        self._session.flush()
        return trade

    def update_status(
        self,
        trade: Trade,
        status: str,
        *,
        broker_order_id: str | None = None,
        filled_quantity: object | None = None,
        filled_avg_price: object | None = None,
        cash_hold_id: uuid.UUID | None = None,
        error_code: str | None = None,
        error_detail: str | None = None,
        submitted_at: datetime | None = None,
        filled_at: datetime | None = None,
    ) -> Trade:
        trade.status = status
        if broker_order_id is not None:
            trade.broker_order_id = broker_order_id
        if filled_quantity is not None:
            trade.filled_quantity = filled_quantity  # type: ignore[assignment]
        if filled_avg_price is not None:
            trade.filled_avg_price = filled_avg_price  # type: ignore[assignment]
        if cash_hold_id is not None:
            trade.cash_hold_id = cash_hold_id
        if error_code is not None:
            trade.error_code = error_code
        if error_detail is not None:
            trade.error_detail = error_detail
        if submitted_at is not None:
            trade.submitted_at = submitted_at
        if filled_at is not None:
            trade.filled_at = filled_at
        self._session.flush()
        return trade

    def apply_update(self, trade_id: uuid.UUID, /, **fields: object) -> None:
        """Blind UPDATE by primary key (used by the async writer; no row load required)."""
        if not fields:
            return
        stmt = update(Trade).where(Trade.id == trade_id).values(**fields)
        self._session.execute(stmt)

    def list_by_status(self, status: str, *, limit: int = 100) -> list[Trade]:
        stmt = (
            select(Trade)
            .where(Trade.status == status)
            .order_by(Trade.created_at)
            .limit(limit)
        )
        return list(self._session.execute(stmt).scalars().all())

    def list_by_statuses(
        self, statuses: Sequence[str], *, is_paper: bool, limit: int = 10_000
    ) -> list[Trade]:
        """Load trades in any of ``statuses`` for one mode (startup rehydration)."""
        stmt = (
            select(Trade)
            .where(Trade.status.in_(statuses), Trade.is_paper == is_paper)
            .order_by(Trade.created_at)
            .limit(limit)
        )
        return list(self._session.execute(stmt).scalars().all())

    def list_closed_between(
        self, start: datetime, end: datetime, *, is_paper: bool, limit: int = 100_000
    ) -> list[Trade]:
        """Load CLOSED trades whose exit landed in ``(start, end]`` for one mode (daily P&L)."""
        stmt = (
            select(Trade)
            .where(
                Trade.is_paper == is_paper,
                Trade.status == TradeStatus.CLOSED.value,
                Trade.closed_at > start,
                Trade.closed_at <= end,
            )
            .order_by(Trade.closed_at)
            .limit(limit)
        )
        return list(self._session.execute(stmt).scalars().all())

    def list_history(
        self,
        *,
        is_paper: bool | None = None,
        status: str | None = None,
        symbol: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Trade]:
        """Most-recent-first trade history with optional mode/status/symbol filters."""
        stmt = select(Trade)
        if is_paper is not None:
            stmt = stmt.where(Trade.is_paper == is_paper)
        if status is not None:
            stmt = stmt.where(Trade.status == status)
        if symbol is not None:
            stmt = stmt.where(Trade.symbol == symbol)
        stmt = stmt.order_by(Trade.created_at.desc()).limit(limit).offset(offset)
        return list(self._session.execute(stmt).scalars().all())

