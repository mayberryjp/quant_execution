"""Unit tests for the execution core: buy/sell lifecycle, exits, reconcile, liquidation.

The Alpaca client is mocked with ``httpx.MockTransport`` (no network); persistence is faked with an
in-memory :class:`FakeWriter` (no database) that applies inserts/updates onto the trade objects, and
the real in-memory :class:`PositionBook` is used so exit/close behavior is exercised end-to-end.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import httpx
import pytest

from quant_execution.clients.alpaca_client import AlpacaBroker
from quant_execution.clients.cash_client import CashHold
from quant_execution.domain.enums import (
    ExecutionMode,
    ExitReason,
    OrderSide,
    PositionType,
    TradeStatus,
)
from quant_execution.domain.exceptions import CashServiceError, ConfigurationError
from quant_execution.domain.positions import Position, PositionBook, PositionState
from quant_execution.domain.schemas import Tick, WatchlistEntry
from quant_execution.domain.services import (
    ExecutionService,
    MarketCloseLiquidator,
    PositionReconciler,
    build_idempotency_key,
    compute_sizing,
    parse_close_time,
    shift_earlier,
)
from quant_execution.kafka.consumer import MessageMeta
from quant_execution.repository.models import Trade

_PROVENANCE = MessageMeta(topic="ticks.paper", partition=0, offset=7)
_LIVE_PROVENANCE = MessageMeta(topic="ticks.live", partition=0, offset=9)


class FakeWriter:
    """Synchronous stand-in for the async writer: applies updates onto the trade objects."""

    def __init__(self) -> None:
        self.trades: dict[uuid.UUID, Trade] = {}

    def insert(self, trade: Trade) -> None:
        self.trades[trade.id] = trade

    def update(self, trade_id: uuid.UUID, /, **fields: object) -> None:
        trade = self.trades[trade_id]
        for name, value in fields.items():
            setattr(trade, name, value)


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


def _broker(handler: httpx.MockTransport, *, attempts: int = 1) -> AlpacaBroker:
    return AlpacaBroker(
        "https://paper-api.alpaca.markets", "key", "secret", max_attempts=attempts, transport=handler
    )


def _ok_transport(order_id: str = "broker-123") -> httpx.MockTransport:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"id": order_id, "status": "accepted"})

    return httpx.MockTransport(handle)


def _entry(sell: Decimal = Decimal(110)) -> WatchlistEntry:
    return WatchlistEntry(
        symbol="AAPL",
        buy_price=Decimal(100),
        sell_price=sell,
        position_type=PositionType.LONG,
        trigger_reason="dip",
    )


def _service(
    mode: ExecutionMode,
    *,
    cash: FakeCash | None = None,
    transport: httpx.MockTransport | None = None,
) -> tuple[ExecutionService, FakeWriter, PositionBook]:
    writer = FakeWriter()
    book = PositionBook()
    service = ExecutionService(
        mode,
        _broker(transport or _ok_transport()),
        writer,  # type: ignore[arg-type]
        book,
        notional_usd=100.0,
        quantity=None,
        cash=cash,  # type: ignore[arg-type]
    )
    return service, writer, book


# --- sizing / idempotency ------------------------------------------------------------------


def test_build_idempotency_key_uses_utc_date() -> None:
    tick = Tick(symbol="AAPL", price=Decimal(99), ts=datetime(2024, 1, 2, 12, 0, tzinfo=UTC))
    assert build_idempotency_key(ExecutionMode.PAPER, _entry(), tick) == "paper:AAPL:dip:2024-01-02"


def test_compute_sizing_by_notional() -> None:
    qty, notional = compute_sizing(Decimal(50), notional_usd=100.0, quantity=None)
    assert qty == Decimal(2)
    assert notional == Decimal(100)


def test_compute_sizing_rounds_down_to_whole_shares() -> None:
    # $100 budget at $30 buys 3 whole shares (3.33 floored); notional reflects the 3 shares.
    qty, notional = compute_sizing(Decimal(30), notional_usd=100.0, quantity=None)
    assert qty == Decimal(3)
    assert notional == Decimal(90)

    # A fractional configured quantity is floored too.
    qty, notional = compute_sizing(Decimal(20), notional_usd=None, quantity=3.7)
    assert qty == Decimal(3)
    assert notional == Decimal(60)


def test_compute_sizing_requires_exactly_one() -> None:
    with pytest.raises(ConfigurationError):
        compute_sizing(Decimal(50), notional_usd=None, quantity=None)
    with pytest.raises(ConfigurationError):
        compute_sizing(Decimal(50), notional_usd=100.0, quantity=3.0)


def test_parse_close_time() -> None:
    assert parse_close_time("") is None
    parsed = parse_close_time("16:00")
    assert parsed is not None and parsed.hour == 16 and parsed.minute == 0
    with pytest.raises(ConfigurationError):
        parse_close_time("nope")


def test_shift_earlier() -> None:
    assert shift_earlier(None, 15) is None
    assert shift_earlier(parse_close_time("11:00"), 15) == parse_close_time("10:45")
    assert shift_earlier(parse_close_time("22:30"), 15) == parse_close_time("22:15")
    # Wraps past midnight.
    assert shift_earlier(parse_close_time("00:05"), 15) == parse_close_time("23:50")


# --- entry (buy) ---------------------------------------------------------------------------


def test_paper_execute_opens_filled_position() -> None:
    service, _writer, book = _service(ExecutionMode.PAPER)
    tick = Tick(symbol="AAPL", price=Decimal(50))
    trade = service.execute(tick, _entry(), provenance=_PROVENANCE)

    assert trade is not None
    assert trade.status == TradeStatus.FILLED.value
    assert trade.side == OrderSide.BUY.value
    assert trade.quantity == Decimal(2)
    assert trade.target_sell_price == Decimal(110)
    assert trade.broker_order_id is None
    assert trade.filled_avg_price == Decimal(50)
    assert len(book) == 1
    positions = book.pending_and_exiting()  # none pending; all OPEN
    assert positions == []


def test_execute_dedup_is_noop() -> None:
    service, _writer, book = _service(ExecutionMode.PAPER)
    tick = Tick(symbol="AAPL", price=Decimal(50))
    assert service.execute(tick, _entry(), provenance=_PROVENANCE) is not None
    assert service.execute(tick, _entry(), provenance=_PROVENANCE) is None
    assert len(book) == 1


def test_broker_error_sets_failed_and_opens_no_position() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"message": "bad"})

    cash = FakeCash(Decimal(1000))
    service, _writer, book = _service(
        ExecutionMode.LIVE, cash=cash, transport=httpx.MockTransport(handle)
    )
    trade = service.execute(
        Tick(symbol="AAPL", price=Decimal(50)), _entry(), provenance=_LIVE_PROVENANCE
    )

    assert trade is not None
    assert trade.status == TradeStatus.FAILED.value
    assert trade.error_code == "BROKER_ERROR"
    assert len(book) == 0


def test_live_requires_cash_client() -> None:
    with pytest.raises(ConfigurationError):
        _service(ExecutionMode.LIVE)


def test_live_sufficient_funds_holds_and_submits() -> None:
    cash = FakeCash(Decimal(1000))
    service, _writer, book = _service(ExecutionMode.LIVE, cash=cash)
    trade = service.execute(
        Tick(symbol="AAPL", price=Decimal(50)), _entry(), provenance=_LIVE_PROVENANCE
    )

    assert trade is not None
    assert trade.status == TradeStatus.SUBMITTED.value  # not filled until reconciled
    assert trade.cash_hold_id == cash.hold_id
    assert trade.filled_at is None
    assert cash.placed == [Decimal(100)]
    assert len(book) == 1
    position = book.pending_and_exiting()[0]
    assert position.state is PositionState.PENDING_ENTRY


def test_live_insufficient_funds_does_not_submit() -> None:
    cash = FakeCash(Decimal(50))
    service, _writer, book = _service(ExecutionMode.LIVE, cash=cash)
    trade = service.execute(
        Tick(symbol="AAPL", price=Decimal(50)), _entry(), provenance=_LIVE_PROVENANCE
    )

    assert trade is not None
    assert trade.status == TradeStatus.INSUFFICIENT_FUNDS.value
    assert cash.placed == []
    assert len(book) == 0


def test_live_broker_error_releases_hold() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"message": "bad"})

    cash = FakeCash(Decimal(1000))
    service, _writer, book = _service(
        ExecutionMode.LIVE, cash=cash, transport=httpx.MockTransport(handle)
    )
    trade = service.execute(
        Tick(symbol="AAPL", price=Decimal(50)), _entry(), provenance=_LIVE_PROVENANCE
    )

    assert trade is not None
    assert trade.status == TradeStatus.FAILED.value
    assert cash.released == [cash.hold_id]
    assert len(book) == 0


# --- exit (sell) ---------------------------------------------------------------------------


def test_paper_target_price_exit_closes_position() -> None:
    service, _writer, book = _service(ExecutionMode.PAPER)
    entry_trade = service.execute(
        Tick(symbol="AAPL", price=Decimal(50)), _entry(sell=Decimal(55)), provenance=_PROVENANCE
    )
    assert entry_trade is not None

    service.check_exits(Tick(symbol="AAPL", price=Decimal(56)))  # 56 >= 55 target

    assert entry_trade.status == TradeStatus.CLOSED.value
    assert entry_trade.exit_reason == ExitReason.TARGET_PRICE.value
    assert entry_trade.exit_filled_avg_price == Decimal(56)
    assert entry_trade.exit_broker_order_id is None
    assert len(book) == 0


def test_exit_not_triggered_below_target() -> None:
    service, _writer, book = _service(ExecutionMode.PAPER)
    service.execute(
        Tick(symbol="AAPL", price=Decimal(50)), _entry(sell=Decimal(55)), provenance=_PROVENANCE
    )
    service.check_exits(Tick(symbol="AAPL", price=Decimal(54)))  # below target
    assert len(book) == 1


def test_paper_liquidate_all_flattens_positions() -> None:
    service, _writer, book = _service(ExecutionMode.PAPER)
    trade = service.execute(
        Tick(symbol="AAPL", price=Decimal(50)), _entry(), provenance=_PROVENANCE
    )
    assert trade is not None
    service.record_price("AAPL", Decimal(60))

    count = service.liquidate_all(ExitReason.MARKET_CLOSE)

    assert count == 1
    assert trade.status == TradeStatus.CLOSED.value
    assert trade.exit_reason == ExitReason.MARKET_CLOSE.value
    assert trade.exit_filled_avg_price == Decimal(60)
    assert len(book) == 0


def test_market_close_liquidator_fires_once_per_day() -> None:
    service, _writer, book = _service(ExecutionMode.PAPER)
    service.execute(Tick(symbol="AAPL", price=Decimal(50)), _entry(), provenance=_PROVENANCE)
    liquidator = MarketCloseLiquidator(service, parse_close_time("16:00"), ZoneInfo("UTC"))

    before = datetime(2026, 8, 28, 15, 0, tzinfo=ZoneInfo("UTC"))
    after = datetime(2026, 8, 28, 16, 30, tzinfo=ZoneInfo("UTC"))
    assert liquidator.maybe_liquidate(now=before) == 0
    assert liquidator.maybe_liquidate(now=after) == 1
    assert liquidator.maybe_liquidate(now=after) == 0  # already ran today
    assert len(book) == 0


# --- live reconciliation -------------------------------------------------------------------


def _reconcile_broker(orders: dict[str, dict[str, object]]) -> AlpacaBroker:
    def handle(request: httpx.Request) -> httpx.Response:
        order_id = request.url.path.rsplit("/", 1)[-1]
        return httpx.Response(200, json=orders[order_id])

    return _broker(httpx.MockTransport(handle))


def _live_position(state: PositionState, *, hold_id: uuid.UUID | None = None) -> Position:
    return Position(
        trade_id=uuid.uuid4(),
        symbol="AAPL",
        position_type=PositionType.LONG,
        quantity=Decimal(2),
        entry_price=Decimal(50),
        sell_price=Decimal(60),
        is_paper=False,
        idempotency_key="live:AAPL:dip:2026-08-28",
        broker_order_id="entry-1",
        exit_broker_order_id="exit-1" if state is PositionState.EXITING else None,
        cash_hold_id=hold_id,
        state=state,
    )


def _seed(writer: FakeWriter, position: Position, status: str) -> Trade:
    trade = Trade(id=position.trade_id, status=status)
    writer.trades[position.trade_id] = trade
    return trade


def test_reconcile_entry_fill_opens_and_captures() -> None:
    writer = FakeWriter()
    book = PositionBook()
    cash = FakeCash(Decimal(0))
    hold_id = uuid.uuid4()
    position = _live_position(PositionState.PENDING_ENTRY, hold_id=hold_id)
    book.open(position)
    trade = _seed(writer, position, TradeStatus.SUBMITTED.value)
    orders = {"entry-1": {"id": "entry-1", "status": "filled", "filled_qty": "2", "filled_avg_price": "50"}}

    checked = PositionReconciler(
        _reconcile_broker(orders), cash, writer, book  # type: ignore[arg-type]
    ).reconcile_once()

    assert checked == 1
    assert trade.status == TradeStatus.FILLED.value
    assert position.state is PositionState.OPEN
    assert cash.captured == [hold_id]


def test_reconcile_entry_reject_removes_and_releases() -> None:
    writer = FakeWriter()
    book = PositionBook()
    cash = FakeCash(Decimal(0))
    hold_id = uuid.uuid4()
    position = _live_position(PositionState.PENDING_ENTRY, hold_id=hold_id)
    book.open(position)
    trade = _seed(writer, position, TradeStatus.SUBMITTED.value)
    orders = {"entry-1": {"id": "entry-1", "status": "rejected"}}

    PositionReconciler(
        _reconcile_broker(orders), cash, writer, book  # type: ignore[arg-type]
    ).reconcile_once()

    assert trade.status == TradeStatus.REJECTED.value
    assert cash.released == [hold_id]
    assert len(book) == 0


def test_reconcile_exit_fill_closes_position() -> None:
    writer = FakeWriter()
    book = PositionBook()
    cash = FakeCash(Decimal(0))
    position = _live_position(PositionState.EXITING)
    book.open(position)
    trade = _seed(writer, position, TradeStatus.EXIT_SUBMITTED.value)
    orders = {"exit-1": {"id": "exit-1", "status": "filled", "filled_qty": "2", "filled_avg_price": "62"}}

    PositionReconciler(
        _reconcile_broker(orders), cash, writer, book  # type: ignore[arg-type]
    ).reconcile_once()

    assert trade.status == TradeStatus.CLOSED.value
    assert trade.exit_filled_avg_price == Decimal(62)
    assert len(book) == 0


def test_reconcile_exit_reject_reopens_position() -> None:
    writer = FakeWriter()
    book = PositionBook()
    cash = FakeCash(Decimal(0))
    position = _live_position(PositionState.EXITING)
    book.open(position)
    trade = _seed(writer, position, TradeStatus.EXIT_SUBMITTED.value)
    orders = {"exit-1": {"id": "exit-1", "status": "canceled"}}

    PositionReconciler(
        _reconcile_broker(orders), cash, writer, book  # type: ignore[arg-type]
    ).reconcile_once()

    assert trade.status == TradeStatus.EXIT_FAILED.value
    assert position.state is PositionState.OPEN
    assert position.exit_broker_order_id is None
    assert len(book) == 1
