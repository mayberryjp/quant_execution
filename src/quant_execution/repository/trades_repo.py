"""Data-access helpers for the ``trades`` table.

All access goes through the SQLAlchemy ORM / Core, which parameterizes values; raw string SQL
interpolation is never used (SPEC.md §3.1, backend standards).
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

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

    def list_by_status(self, status: str, *, limit: int = 100) -> list[Trade]:
        stmt = (
            select(Trade)
            .where(Trade.status == status)
            .order_by(Trade.created_at)
            .limit(limit)
        )
        return list(self._session.execute(stmt).scalars().all())
