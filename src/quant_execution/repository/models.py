"""ORM models owned by this service.

The service owns only the ``trades`` table (SPEC.md §3.1 / §3.2).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    CHAR,
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from quant_execution.db import Base

__all__ = ["Base", "DailyPnl", "Trade"]

_NUM = Numeric(20, 8)


class Trade(Base):
    """A single order and its lifecycle, per SPEC.md §3.2."""

    __tablename__ = "trades"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    execution_mode: Mapped[str] = mapped_column(String(8), nullable=False)
    is_paper: Mapped[bool] = mapped_column(Boolean, nullable=False)

    symbol: Mapped[str] = mapped_column(String(16), nullable=False)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    position_type: Mapped[str] = mapped_column(String(8), nullable=False)

    target_buy_price: Mapped[Decimal] = mapped_column(_NUM, nullable=False)
    trigger_price: Mapped[Decimal] = mapped_column(_NUM, nullable=False)
    quantity: Mapped[Decimal] = mapped_column(_NUM, nullable=False)
    notional: Mapped[Decimal] = mapped_column(_NUM, nullable=False)
    currency: Mapped[str] = mapped_column(CHAR(3), nullable=False, default="USD")

    status: Mapped[str] = mapped_column(String(24), nullable=False)
    broker: Mapped[str] = mapped_column(String(16), nullable=False)
    broker_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    filled_quantity: Mapped[Decimal | None] = mapped_column(_NUM, nullable=True)
    filled_avg_price: Mapped[Decimal | None] = mapped_column(_NUM, nullable=True)

    # Exit / sell side. The entry row is updated in place when the position is closed
    # (target price hit or market-close liquidation); see SPEC.md §5.7.
    target_sell_price: Mapped[Decimal | None] = mapped_column(_NUM, nullable=True)
    exit_broker_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    exit_filled_quantity: Mapped[Decimal | None] = mapped_column(_NUM, nullable=True)
    exit_filled_avg_price: Mapped[Decimal | None] = mapped_column(_NUM, nullable=True)
    exit_reason: Mapped[str | None] = mapped_column(String(16), nullable=True)

    cash_hold_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    source_query_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    trigger_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)

    idempotency_key: Mapped[str] = mapped_column(String(200), nullable=False)

    kafka_topic: Mapped[str | None] = mapped_column(String(128), nullable=True)
    kafka_partition: Mapped[int | None] = mapped_column(Integer, nullable=True)
    kafka_offset: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    submitted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    filled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    exit_submitted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    closed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        Index("idx_trades_symbol", "symbol"),
        Index("idx_trades_status", "status"),
        Index("idx_trades_is_paper", "is_paper"),
        Index("idx_trades_created_at", "created_at"),
        Index("uq_trades_idempotency_key", "idempotency_key", unique=True),
    )


class DailyPnl(Base):
    """End-of-day profit-and-loss summary for one trading day and mode (paper/live).

    One row per ``(trade_date, is_paper)`` is written by the daily P&L reporter after each mode's
    market close, aggregating every trade that closed during that session.
    """

    __tablename__ = "daily_pnl"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    execution_mode: Mapped[str] = mapped_column(String(8), nullable=False)
    is_paper: Mapped[bool] = mapped_column(Boolean, nullable=False)

    total_trades: Mapped[int] = mapped_column(Integer, nullable=False)
    winning_trades: Mapped[int] = mapped_column(Integer, nullable=False)
    losing_trades: Mapped[int] = mapped_column(Integer, nullable=False)
    breakeven_trades: Mapped[int] = mapped_column(Integer, nullable=False)
    symbols_traded: Mapped[int] = mapped_column(Integer, nullable=False)

    realized_pnl: Mapped[Decimal] = mapped_column(_NUM, nullable=False)
    amount_invested: Mapped[Decimal] = mapped_column(_NUM, nullable=False)
    gross_proceeds: Mapped[Decimal] = mapped_column(_NUM, nullable=False)
    win_rate: Mapped[Decimal] = mapped_column(_NUM, nullable=False)
    return_pct: Mapped[Decimal] = mapped_column(_NUM, nullable=False)
    average_win: Mapped[Decimal] = mapped_column(_NUM, nullable=False)
    average_loss: Mapped[Decimal] = mapped_column(_NUM, nullable=False)
    largest_win: Mapped[Decimal] = mapped_column(_NUM, nullable=False)
    largest_loss: Mapped[Decimal] = mapped_column(_NUM, nullable=False)
    currency: Mapped[str] = mapped_column(CHAR(3), nullable=False, default="USD")

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    __table_args__ = (
        UniqueConstraint("trade_date", "is_paper", name="uq_daily_pnl_date_mode"),
        Index("idx_daily_pnl_trade_date", "trade_date"),
        Index("idx_daily_pnl_is_paper", "is_paper"),
    )
