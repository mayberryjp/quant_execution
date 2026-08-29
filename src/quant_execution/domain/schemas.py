"""API-facing schemas (Pydantic) for the domain."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from quant_execution.domain.enums import OrderSide, PositionType
from quant_execution.repository.models import Trade


class WatchlistEntry(BaseModel):
    """A single armed watchlist entry from ``quant_stickynote`` (SPEC.md §4.1)."""

    symbol: str
    buy_price: Decimal
    sell_price: Decimal
    position_type: PositionType
    source_query_id: str | None = None
    trigger_reason: str | None = None
    status: str = "active"


class Tick(BaseModel):
    """A single streaming price tick consumed from Kafka (SPEC.md §5.1).

    The paper streamingchart emits OHLC bars (``ticker``/``close``/``bar_time``); the bar close is
    taken as the current price. Aliases accept that shape while ignoring the other bar fields
    (``schema_version``, ``sequence``, ``open``/``high``/``low``, ``volume``, ...).
    """

    model_config = ConfigDict(populate_by_name=True)

    symbol: str = Field(validation_alias=AliasChoices("symbol", "ticker"))
    price: Decimal = Field(validation_alias=AliasChoices("price", "close"))
    ts: datetime | None = Field(default=None, validation_alias=AliasChoices("ts", "bar_time"))

    @classmethod
    def parse(cls, raw: bytes | str) -> Tick:
        """Parse a raw Kafka message value (JSON) into a validated tick."""
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return cls.model_validate_json(raw)


class OrderIntent(BaseModel):
    """A matched (tick, entry) pair to be executed (SPEC.md §5.1). Logged in SLICE 3."""

    symbol: str
    side: OrderSide
    position_type: PositionType
    target_buy_price: Decimal
    trigger_price: Decimal
    trigger_reason: str | None = None
    source_query_id: str | None = None

    @classmethod
    def from_match(cls, entry: WatchlistEntry, tick: Tick) -> OrderIntent:
        return cls(
            symbol=entry.symbol,
            side=entry.position_type.entry_side,
            position_type=entry.position_type,
            target_buy_price=entry.buy_price,
            trigger_price=tick.price,
            trigger_reason=entry.trigger_reason,
            source_query_id=entry.source_query_id,
        )


class TradeResponse(BaseModel):
    """Read model returned by the API for a persisted trade."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    execution_mode: str
    is_paper: bool
    symbol: str
    side: str
    position_type: str
    target_buy_price: Decimal
    trigger_price: Decimal
    quantity: Decimal
    notional: Decimal
    currency: str
    status: str
    broker: str
    broker_order_id: str | None
    filled_quantity: Decimal | None
    filled_avg_price: Decimal | None
    target_sell_price: Decimal | None
    exit_broker_order_id: str | None
    exit_filled_quantity: Decimal | None
    exit_filled_avg_price: Decimal | None
    exit_reason: str | None
    cash_hold_id: uuid.UUID | None
    source_query_id: str | None
    trigger_reason: str | None
    idempotency_key: str
    error_code: str | None
    error_detail: str | None
    created_at: datetime
    submitted_at: datetime | None
    filled_at: datetime | None
    exit_submitted_at: datetime | None
    closed_at: datetime | None
    updated_at: datetime

    @classmethod
    def from_orm_trade(cls, trade: Trade) -> TradeResponse:
        return cls.model_validate(trade)
