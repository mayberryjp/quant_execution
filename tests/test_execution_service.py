"""Unit tests for the execution core (SLICE 4): sizing, idempotency, paper fill, broker error.

The Alpaca client is mocked with ``httpx.MockTransport`` (no network) and persistence is faked with
an in-memory unit-of-work (no database), so these tests run without external services.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from quant_execution.clients.alpaca_client import AlpacaBroker
from quant_execution.clients.cash_client import CashHold
from quant_execution.domain.enums import ExecutionMode, OrderSide, PositionType, TradeStatus
from quant_execution.domain.exceptions import BrokerError, CashServiceError, ConfigurationError
from quant_execution.domain.schemas import Tick, WatchlistEntry
from quant_execution.domain.services import (
    ExecutionService,
    ReconciliationService,
    build_idempotency_key,
    compute_sizing,
)
from quant_execution.kafka.consumer import MessageMeta
from quant_execution.repository.models import Trade

_PROVENANCE = MessageMeta(topic="ticks.paper", partition=0, offset=7)
_LIVE_PROVENANCE = MessageMeta(topic="ticks.live", partition=0, offset=9)



class FakeRepo:
    def __init__(self, existing: set[str] | None = None) -> None:
        self.existing = existing or set()
        self.inserted: list[Trade] = []

    def exists_by_idempotency_key(self, idempotency_key: str) -> bool:
        return idempotency_key in self.existing

    def insert(self, trade: Trade) -> Trade:
        self.inserted.append(trade)
        self.existing.add(trade.idempotency_key)
        return trade


def _uow_factory(repo: FakeRepo):
    @contextmanager
    def factory() -> Iterator[FakeRepo]:
        yield repo

    return factory


def _broker(handler: httpx.MockTransport, *, attempts: int = 1) -> AlpacaBroker:
    return AlpacaBroker(
        "https://paper-api.alpaca.markets",
        "key",
        "secret",
        max_attempts=attempts,
        transport=handler,
    )


def _ok_transport() -> httpx.MockTransport:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"id": "broker-123", "status": "accepted"})

    return httpx.MockTransport(handle)


def _entry() -> WatchlistEntry:
    return WatchlistEntry(
        symbol="AAPL",
        buy_price=Decimal(100),
        position_type=PositionType.LONG,
        trigger_reason="dip",
    )


def test_build_idempotency_key_uses_utc_date() -> None:
    tick = Tick(symbol="AAPL", price=Decimal(99), ts=datetime(2024, 1, 2, 12, 0, tzinfo=UTC))
    key = build_idempotency_key(ExecutionMode.PAPER, _entry(), tick)
    assert key == "paper:AAPL:dip:2024-01-02"


def test_compute_sizing_by_notional() -> None:
    qty, notional = compute_sizing(Decimal(50), notional_usd=100.0, quantity=None)
    assert qty == Decimal(2)
    assert notional == Decimal(100)


def test_compute_sizing_by_quantity() -> None:
    qty, notional = compute_sizing(Decimal(50), notional_usd=None, quantity=3.0)
    assert qty == Decimal(3)
    assert notional == Decimal(150)


def test_compute_sizing_requires_exactly_one() -> None:
    with pytest.raises(ConfigurationError):
        compute_sizing(Decimal(50), notional_usd=None, quantity=None)
    with pytest.raises(ConfigurationError):
        compute_sizing(Decimal(50), notional_usd=100.0, quantity=3.0)


def test_paper_execute_records_filled_trade() -> None:
    repo = FakeRepo()
    service = ExecutionService(
        ExecutionMode.PAPER,
        _broker(_ok_transport()),
        notional_usd=100.0,
        quantity=None,
        unit_of_work=_uow_factory(repo),
    )
    tick = Tick(symbol="AAPL", price=Decimal(50))
    trade = service.execute(tick, _entry(), provenance=_PROVENANCE)

    assert trade is not None
    assert trade.is_paper is True
    assert trade.status == TradeStatus.FILLED.value
    assert trade.side == OrderSide.BUY.value
    assert trade.quantity == Decimal(2)
    assert trade.notional == Decimal(100)
    assert trade.broker_order_id == "broker-123"
    assert trade.filled_quantity == Decimal(2)
    assert trade.filled_avg_price == Decimal(50)
    assert trade.kafka_offset == 7
    assert len(repo.inserted) == 1


def test_execute_dedup_is_noop() -> None:
    tick = Tick(symbol="AAPL", price=Decimal(50))
    key = build_idempotency_key(ExecutionMode.PAPER, _entry(), tick)
    repo = FakeRepo(existing={key})
    service = ExecutionService(
        ExecutionMode.PAPER,
        _broker(_ok_transport()),
        notional_usd=100.0,
        quantity=None,
        unit_of_work=_uow_factory(repo),
    )
    assert service.execute(tick, _entry(), provenance=_PROVENANCE) is None
    assert repo.inserted == []


def test_broker_error_sets_failed() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"message": "bad"})

    repo = FakeRepo()
    service = ExecutionService(
        ExecutionMode.PAPER,
        _broker(httpx.MockTransport(handle)),
        notional_usd=100.0,
        quantity=None,
        unit_of_work=_uow_factory(repo),
    )
    tick = Tick(symbol="AAPL", price=Decimal(50))
    trade = service.execute(tick, _entry(), provenance=_PROVENANCE)

    assert trade is not None
    assert trade.status == TradeStatus.FAILED.value
    assert trade.error_code == "BROKER_ERROR"
    assert trade.broker_order_id is None


def test_alpaca_submit_order_raises_broker_error_on_server_error() -> None:
    calls = {"n": 0}

    def handle(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(500, json={"message": "boom"})

    broker = _broker(httpx.MockTransport(handle), attempts=2)
    with pytest.raises(BrokerError):
        broker.submit_order(
            symbol="AAPL", qty=Decimal(1), side=OrderSide.BUY, client_order_id="k"
        )
    assert calls["n"] == 2


# --- Live path (SLICE 5) -------------------------------------------------------------------


class FakeCash:
    def __init__(self, balance: Decimal, *, fail_balance: bool = False) -> None:
        self.balance = balance
        self.fail_balance = fail_balance
        self.hold_id = uuid.uuid4()
        self.placed: list[Decimal] = []
        self.captured: list[uuid.UUID] = []
        self.released: list[uuid.UUID] = []

    def get_available_balance(self) -> Decimal:
        if self.fail_balance:
            raise CashServiceError("balance service down")
        return self.balance

    def place_hold(self, amount: Decimal, *, reason: str, reference_id: str) -> CashHold:
        self.placed.append(amount)
        return CashHold(id=self.hold_id)

    def capture_hold(self, hold_id: uuid.UUID) -> None:
        self.captured.append(hold_id)

    def release_hold(self, hold_id: uuid.UUID) -> None:
        self.released.append(hold_id)


def _live_service(repo: FakeRepo, cash: FakeCash, transport: httpx.MockTransport) -> ExecutionService:
    return ExecutionService(
        ExecutionMode.LIVE,
        _broker(transport),
        notional_usd=100.0,
        quantity=None,
        cash=cash,  # type: ignore[arg-type]
        unit_of_work=_uow_factory(repo),
    )


def test_live_requires_cash_client() -> None:
    with pytest.raises(ConfigurationError):
        ExecutionService(
            ExecutionMode.LIVE, _broker(_ok_transport()), notional_usd=100.0, quantity=None
        )


def test_live_sufficient_funds_holds_and_submits() -> None:
    repo = FakeRepo()
    cash = FakeCash(Decimal(1000))
    service = _live_service(repo, cash, _ok_transport())
    tick = Tick(symbol="AAPL", price=Decimal(50))
    trade = service.execute(tick, _entry(), provenance=_LIVE_PROVENANCE)

    assert trade is not None
    assert trade.is_paper is False
    assert trade.status == TradeStatus.SUBMITTED.value
    assert trade.cash_hold_id == cash.hold_id
    assert trade.filled_at is None
    assert cash.placed == [Decimal(100)]
    assert cash.captured == []


def test_live_insufficient_funds_does_not_submit() -> None:
    repo = FakeRepo()
    cash = FakeCash(Decimal(50))
    service = _live_service(repo, cash, _ok_transport())
    tick = Tick(symbol="AAPL", price=Decimal(50))
    trade = service.execute(tick, _entry(), provenance=_LIVE_PROVENANCE)

    assert trade is not None
    assert trade.status == TradeStatus.INSUFFICIENT_FUNDS.value
    assert trade.broker_order_id is None
    assert cash.placed == []


def test_live_broker_error_releases_hold() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"message": "bad"})

    repo = FakeRepo()
    cash = FakeCash(Decimal(1000))
    service = _live_service(repo, cash, httpx.MockTransport(handle))
    tick = Tick(symbol="AAPL", price=Decimal(50))
    trade = service.execute(tick, _entry(), provenance=_LIVE_PROVENANCE)

    assert trade is not None
    assert trade.status == TradeStatus.FAILED.value
    assert trade.error_code == "BROKER_ERROR"
    assert cash.placed == [Decimal(100)]
    assert cash.released == [cash.hold_id]


# --- Reconciliation (SLICE 5, §5.6) --------------------------------------------------------


class FakeReconcileRepo:
    def __init__(self, trades: list[Trade]) -> None:
        self.trades = trades

    def list_by_status(self, status: str, *, limit: int = 100) -> list[Trade]:
        return [t for t in self.trades if t.status == status]

    def update_status(
        self,
        trade: Trade,
        status: str,
        *,
        filled_quantity: object | None = None,
        filled_avg_price: object | None = None,
        filled_at: datetime | None = None,
        **_: object,
    ) -> Trade:
        trade.status = status
        if filled_quantity is not None:
            trade.filled_quantity = filled_quantity  # type: ignore[assignment]
        if filled_avg_price is not None:
            trade.filled_avg_price = filled_avg_price  # type: ignore[assignment]
        if filled_at is not None:
            trade.filled_at = filled_at
        return trade


def _reconcile_uow(repo: FakeReconcileRepo):
    @contextmanager
    def factory() -> Iterator[FakeReconcileRepo]:
        yield repo

    return factory


def _open_trade(status: str, broker_order_id: str, hold_id: uuid.UUID | None) -> Trade:
    return Trade(
        status=status,
        broker_order_id=broker_order_id,
        cash_hold_id=hold_id,
    )


def _reconcile_broker(orders: dict[str, dict[str, object]]) -> AlpacaBroker:
    def handle(request: httpx.Request) -> httpx.Response:
        order_id = request.url.path.rsplit("/", 1)[-1]
        return httpx.Response(200, json=orders[order_id])

    return _broker(httpx.MockTransport(handle))


def _reconciler(repo: FakeReconcileRepo, cash: FakeCash, orders: dict[str, dict[str, object]]) -> ReconciliationService:
    return ReconciliationService(
        _reconcile_broker(orders),
        cash,  # type: ignore[arg-type]
        unit_of_work=_reconcile_uow(repo),  # type: ignore[arg-type]
    )


def test_reconcile_fill_captures_hold() -> None:
    hold_id = uuid.uuid4()
    trade = _open_trade(TradeStatus.SUBMITTED.value, "o1", hold_id)
    repo = FakeReconcileRepo([trade])
    cash = FakeCash(Decimal(0))
    orders = {"o1": {"id": "o1", "status": "filled", "filled_qty": "2", "filled_avg_price": "101"}}
    checked = _reconciler(repo, cash, orders).reconcile_once()

    assert checked == 1
    assert trade.status == TradeStatus.FILLED.value
    assert trade.filled_quantity == Decimal(2)
    assert trade.filled_avg_price == Decimal(101)
    assert cash.captured == [hold_id]
    assert cash.released == []


def test_reconcile_reject_releases_hold() -> None:
    hold_id = uuid.uuid4()
    trade = _open_trade(TradeStatus.SUBMITTED.value, "o2", hold_id)
    repo = FakeReconcileRepo([trade])
    cash = FakeCash(Decimal(0))
    orders = {"o2": {"id": "o2", "status": "rejected"}}
    _reconciler(repo, cash, orders).reconcile_once()

    assert trade.status == TradeStatus.REJECTED.value
    assert cash.released == [hold_id]
    assert cash.captured == []


def test_reconcile_partial_fill_stays_open() -> None:
    hold_id = uuid.uuid4()
    trade = _open_trade(TradeStatus.SUBMITTED.value, "o3", hold_id)
    repo = FakeReconcileRepo([trade])
    cash = FakeCash(Decimal(0))
    orders = {"o3": {"id": "o3", "status": "partially_filled", "filled_qty": "1"}}
    _reconciler(repo, cash, orders).reconcile_once()

    assert trade.status == TradeStatus.PARTIALLY_FILLED.value
    assert trade.filled_quantity == Decimal(1)
    assert cash.captured == []
    assert cash.released == []

