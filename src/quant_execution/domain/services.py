"""Execution core: full buy → hold → sell lifecycle (SPEC.md §5.5–§5.7).

The service is mode-agnostic; the two modes share one code path and differ only in the documented
ways (topic, ``is_paper``, live cash check, and fill confirmation). The streaming hot path is
in-memory only: entries are matched against the watchlist and exits against the in-memory
:class:`PositionBook`; every durable write is handed to a non-blocking :class:`TradeWriter`. A
position's entry row is updated in place when it closes (target price hit or market-close
liquidation). Live fills are confirmed by a background reconciler; a per-mode liquidator flattens
all open positions at the configured market-close time so nothing is carried past the session.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime
from datetime import time as clock_time
from decimal import Decimal
from zoneinfo import ZoneInfo

from quant_execution.clients.alpaca_client import AlpacaBroker, AlpacaOrder
from quant_execution.clients.cash_client import CashClient
from quant_execution.domain.enums import ExecutionMode, ExitReason, PositionType, TradeStatus
from quant_execution.domain.exceptions import BrokerError, CashServiceError, ConfigurationError
from quant_execution.domain.positions import Position, PositionBook, PositionState
from quant_execution.domain.schemas import Tick, WatchlistEntry
from quant_execution.kafka.consumer import MessageMeta
from quant_execution.logging import format_event, get_logger
from quant_execution.repository.async_writer import TradeWriter
from quant_execution.repository.models import Trade

logger = get_logger(__name__)

# Alpaca order statuses mapped to terminal/non-terminal outcomes (SPEC.md §5.6).
_ALPACA_FILLED = frozenset({"filled"})
_ALPACA_PARTIAL = frozenset({"partially_filled"})
_ALPACA_DEAD = frozenset({"canceled", "cancelled", "rejected", "expired", "done_for_day"})

# Trade statuses whose positions are rebuilt into the book at startup (SPEC.md §5.7).
REHYDRATE_STATUSES: tuple[str, ...] = (
    TradeStatus.CASH_HELD.value,
    TradeStatus.SUBMITTED.value,
    TradeStatus.PARTIALLY_FILLED.value,
    TradeStatus.FILLED.value,
    TradeStatus.EXIT_SUBMITTED.value,
    TradeStatus.EXIT_PARTIALLY_FILLED.value,
)
_OPEN_STATUSES = frozenset({TradeStatus.FILLED.value})
_EXITING_STATUSES = frozenset(
    {TradeStatus.EXIT_SUBMITTED.value, TradeStatus.EXIT_PARTIALLY_FILLED.value}
)


def build_idempotency_key(mode: ExecutionMode, entry: WatchlistEntry, tick: Tick) -> str:
    """``{mode}:{symbol}:{trigger_reason}:{signal_date}`` with signal_date as a UTC date."""
    ts = tick.ts.astimezone(UTC) if tick.ts is not None else datetime.now(UTC)
    signal_date = ts.date().isoformat()
    return f"{mode.value}:{entry.symbol}:{entry.trigger_reason or ''}:{signal_date}"


def compute_sizing(
    trigger_price: Decimal,
    *,
    notional_usd: float | None,
    quantity: float | None,
) -> tuple[Decimal, Decimal]:
    """Return ``(quantity, notional)`` from config; exactly one rule must be set (§5.3)."""
    if (notional_usd is None) == (quantity is None):
        raise ConfigurationError(
            "exactly one of EXEC_ORDER_NOTIONAL_USD or EXEC_ORDER_QUANTITY must be set"
        )
    if trigger_price <= 0:
        raise ConfigurationError("trigger price must be positive for sizing")
    if notional_usd is not None:
        notional = Decimal(str(notional_usd))
        return notional / trigger_price, notional
    qty = Decimal(str(quantity))
    return qty, qty * trigger_price


def parse_close_time(raw: str) -> clock_time | None:
    """Parse an ``HH:MM`` market-close string; empty disables liquidation for that mode."""
    if not raw:
        return None
    try:
        hour_str, minute_str = raw.split(":", 1)
        return clock_time(hour=int(hour_str), minute=int(minute_str))
    except (ValueError, TypeError) as exc:
        raise ConfigurationError(f"invalid market close time {raw!r}; expected HH:MM") from exc


def shift_earlier(t: clock_time | None, minutes: int) -> clock_time | None:
    """Return ``t`` moved ``minutes`` earlier (wrapping past midnight); ``None`` passes through."""
    if t is None:
        return None
    total = (t.hour * 60 + t.minute - minutes) % (24 * 60)
    return clock_time(hour=total // 60, minute=total % 60)


def position_from_trade(trade: Trade) -> Position:
    """Rebuild an in-memory position from a persisted open trade (startup rehydration)."""
    if trade.status in _OPEN_STATUSES:
        state = PositionState.OPEN
    elif trade.status in _EXITING_STATUSES:
        state = PositionState.EXITING
    else:
        state = PositionState.PENDING_ENTRY
    return Position(
        trade_id=trade.id,
        symbol=trade.symbol,
        position_type=PositionType(trade.position_type),
        quantity=trade.filled_quantity or trade.quantity,
        entry_price=trade.filled_avg_price or trade.trigger_price,
        sell_price=trade.target_sell_price,
        is_paper=trade.is_paper,
        idempotency_key=trade.idempotency_key,
        broker_order_id=trade.broker_order_id,
        exit_broker_order_id=trade.exit_broker_order_id,
        cash_hold_id=trade.cash_hold_id,
        state=state,
    )


class ExecutionService:
    """Runs the buy/sell lifecycle; in-memory state, non-blocking persistence (SPEC.md §5.7)."""

    def __init__(
        self,
        mode: ExecutionMode,
        broker: AlpacaBroker,
        writer: TradeWriter,
        book: PositionBook,
        *,
        notional_usd: float | None,
        quantity: float | None,
        cash: CashClient | None = None,
    ) -> None:
        if not mode.is_paper and cash is None:
            raise ConfigurationError("live mode requires a cash client")
        self._mode = mode
        self._broker = broker
        self._writer = writer
        self._book = book
        self._notional_usd = notional_usd
        self._quantity = quantity
        self._cash = cash
        self._seen: set[str] = set()
        self._seen_lock = threading.Lock()
        self._last_price: dict[str, Decimal] = {}

    @property
    def book(self) -> PositionBook:
        return self._book

    def seed(self, keys: Iterable[str]) -> None:
        """Pre-load idempotency keys of already-open trades so restarts do not re-buy."""
        with self._seen_lock:
            self._seen.update(keys)

    def forget(self, key: str) -> None:
        """Drop an idempotency key once its position is closed so the signal can be re-entered."""
        with self._seen_lock:
            self._seen.discard(key)

    def record_price(self, symbol: str, price: Decimal) -> None:
        """Track the latest price per symbol for market-close paper fills."""
        self._last_price[symbol] = price

    # -- entry (buy) -----------------------------------------------------------------

    def execute(
        self, tick: Tick, entry: WatchlistEntry, *, provenance: MessageMeta
    ) -> Trade | None:
        """Open a position for one matched signal; returns the entry trade, or ``None`` on dedup."""
        start = time.perf_counter()
        key = build_idempotency_key(self._mode, entry, tick)
        with self._seen_lock:
            if key in self._seen:
                logger.info(format_event("trade_dedup", symbol=entry.symbol, idempotency_key=key))
                return None
            self._seen.add(key)

        qty, notional = compute_sizing(
            tick.price, notional_usd=self._notional_usd, quantity=self._quantity
        )
        trade_id = uuid.uuid4()
        trade = Trade(
            id=trade_id,
            execution_mode=self._mode.value,
            is_paper=self._mode.is_paper,
            symbol=entry.symbol,
            side=entry.position_type.entry_side.value,
            position_type=entry.position_type.value,
            target_buy_price=entry.buy_price,
            target_sell_price=entry.sell_price,
            trigger_price=tick.price,
            quantity=qty,
            notional=notional,
            status=TradeStatus.NEW.value,
            broker=ExecutionMode.PAPER.value if self._mode.is_paper else self._broker.name,
            idempotency_key=key,
            source_query_id=entry.source_query_id,
            trigger_reason=entry.trigger_reason,
            kafka_topic=provenance.topic,
            kafka_partition=provenance.partition,
            kafka_offset=provenance.offset,
        )
        self._writer.insert(trade)

        hold_id: uuid.UUID | None = None
        if not self._mode.is_paper:
            ok, hold_id = self._reserve_cash(trade_id, notional, key)
            if not ok:
                self._log_entry(entry.symbol, key, TradeStatus.INSUFFICIENT_FUNDS.value, None, start)
                return trade

        if self._mode.is_paper:
            # Paper entries fill immediately in-process; no external broker is involved.
            now = datetime.now(UTC)
            position = Position(
                trade_id=trade_id,
                symbol=entry.symbol,
                position_type=entry.position_type,
                quantity=qty,
                entry_price=tick.price,
                sell_price=entry.sell_price,
                is_paper=True,
                idempotency_key=key,
                cash_hold_id=hold_id,
                state=PositionState.OPEN,
            )
            self._writer.update(
                trade_id,
                status=TradeStatus.FILLED.value,
                filled_quantity=qty,
                filled_avg_price=tick.price,
                filled_at=now,
            )
            self._book.open(position)
            self._log_entry(entry.symbol, key, TradeStatus.FILLED.value, None, start)
            return trade

        try:
            order = self._broker.submit_order(
                symbol=entry.symbol,
                qty=qty,
                side=entry.position_type.entry_side,
                client_order_id=key,
            )
        except BrokerError as exc:
            self._writer.update(
                trade_id,
                status=TradeStatus.FAILED.value,
                error_code="BROKER_ERROR",
                error_detail=str(exc),
            )
            self._release_hold_safe(hold_id)
            logger.warning("broker submit failed key=%s: %s", key, exc)
            self._log_entry(entry.symbol, key, TradeStatus.FAILED.value, None, start)
            return trade

        now = datetime.now(UTC)
        self._writer.update(
            trade_id,
            status=TradeStatus.SUBMITTED.value,
            broker_order_id=order.id,
            submitted_at=now,
        )
        position = Position(
            trade_id=trade_id,
            symbol=entry.symbol,
            position_type=entry.position_type,
            quantity=qty,
            entry_price=tick.price,
            sell_price=entry.sell_price,
            is_paper=False,
            idempotency_key=key,
            broker_order_id=order.id,
            cash_hold_id=hold_id,
            state=PositionState.PENDING_ENTRY,
        )
        self._book.open(position)
        self._log_entry(entry.symbol, key, TradeStatus.SUBMITTED.value, order.id, start)
        return trade

    def _reserve_cash(
        self, trade_id: uuid.UUID, notional: Decimal, key: str
    ) -> tuple[bool, uuid.UUID | None]:
        """Live-only: check balance and place a hold. Returns ``(ok, hold_id)``."""
        cash = self._cash
        if cash is None:  # pragma: no cover - guarded at construction
            raise ConfigurationError("live mode requires a cash client")
        try:
            balance = cash.get_available_balance()
            if balance < notional:
                self._writer.update(trade_id, status=TradeStatus.INSUFFICIENT_FUNDS.value)
                logger.info(
                    "insufficient funds key=%s balance=%s notional=%s", key, balance, notional
                )
                return False, None
            hold = cash.place_hold(notional, reason=key, reference_id=key)
        except CashServiceError as exc:
            self._writer.update(
                trade_id,
                status=TradeStatus.FAILED.value,
                error_code="CASH_ERROR",
                error_detail=str(exc),
            )
            logger.warning("cash reservation failed key=%s: %s", key, exc)
            return False, None
        self._writer.update(trade_id, status=TradeStatus.CASH_HELD.value, cash_hold_id=hold.id)
        return True, hold.id

    def _release_hold_safe(self, hold_id: uuid.UUID | None) -> None:
        if self._mode.is_paper or self._cash is None or hold_id is None:
            return
        try:
            self._cash.release_hold(hold_id)
        except CashServiceError as exc:
            logger.warning("failed to release hold %s: %s", hold_id, exc)

    # -- exit (sell) -----------------------------------------------------------------

    def check_exits(self, tick: Tick) -> None:
        """Hot path: submit an exit for any open position whose target price is reached."""
        for position in self._book.claim_exits(tick.symbol, tick.price):
            self._submit_exit(position, tick.price, ExitReason.TARGET_PRICE)

    def liquidate_all(self, reason: ExitReason = ExitReason.MARKET_CLOSE) -> int:
        """Flatten every open position with a market exit; returns how many were submitted."""
        positions = self._book.claim_all_open()
        for position in positions:
            price = self._last_price.get(position.symbol, position.entry_price)
            self._submit_exit(position, price, reason)
        return len(positions)

    def _submit_exit(self, position: Position, price: Decimal, reason: ExitReason) -> None:
        now = datetime.now(UTC)
        if position.is_paper:
            # Paper exits fill immediately in-process; no external broker is involved.
            self._writer.update(
                position.trade_id,
                status=TradeStatus.CLOSED.value,
                exit_reason=reason.value,
                exit_submitted_at=now,
                exit_filled_quantity=position.quantity,
                exit_filled_avg_price=price,
                closed_at=now,
            )
            self._book.remove(position)
            self.forget(position.idempotency_key)
            logger.info(
                format_event(
                    "exit_submitted",
                    symbol=position.symbol,
                    mode=self._mode.value,
                    idempotency_key=position.idempotency_key,
                    broker_order_id=None,
                    reason=reason.value,
                )
            )
            return

        exit_key = f"{position.idempotency_key}:exit"
        try:
            order = self._broker.submit_order(
                symbol=position.symbol,
                qty=position.quantity,
                side=position.exit_side,
                client_order_id=exit_key,
            )
        except BrokerError as exc:
            self._writer.update(
                position.trade_id,
                status=TradeStatus.EXIT_FAILED.value,
                error_code="EXIT_BROKER_ERROR",
                error_detail=str(exc),
            )
            self._book.release_exit(position)
            logger.warning("exit submit failed symbol=%s: %s", position.symbol, exc)
            return

        position.exit_broker_order_id = order.id
        self._writer.update(
            position.trade_id,
            status=TradeStatus.EXIT_SUBMITTED.value,
            exit_broker_order_id=order.id,
            exit_reason=reason.value,
            exit_submitted_at=now,
        )
        logger.info(
            format_event(
                "exit_submitted",
                symbol=position.symbol,
                mode=self._mode.value,
                idempotency_key=position.idempotency_key,
                broker_order_id=order.id,
                reason=reason.value,
            )
        )

    def _log_entry(
        self, symbol: str, key: str, status: str, broker_order_id: str | None, start: float
    ) -> None:
        duration_ms = round((time.perf_counter() - start) * 1000, 2)
        logger.info(
            format_event(
                "trade_executed",
                symbol=symbol,
                mode=self._mode.value,
                idempotency_key=key,
                broker_order_id=broker_order_id,
                status=status,
                duration_ms=duration_ms,
            )
        )


class PositionReconciler:
    """Live-only loop confirming entry and exit fills against the broker (SPEC.md §5.6)."""

    def __init__(
        self,
        broker: AlpacaBroker,
        cash: CashClient,
        writer: TradeWriter,
        book: PositionBook,
        on_close: Callable[[str], None] | None = None,
    ) -> None:
        self._broker = broker
        self._cash = cash
        self._writer = writer
        self._book = book
        self._on_close = on_close

    def reconcile_once(self) -> int:
        """Poll every pending/exiting position once; return how many were checked."""
        checked = 0
        for position in self._book.pending_and_exiting():
            if position.state is PositionState.PENDING_ENTRY:
                if position.broker_order_id is None:
                    continue
                order = self._fetch(position.broker_order_id)
                if order is None:
                    continue
                self._apply_entry(position, order)
            elif position.state is PositionState.EXITING:
                if position.exit_broker_order_id is None:
                    continue
                order = self._fetch(position.exit_broker_order_id)
                if order is None:
                    continue
                self._apply_exit(position, order)
            checked += 1
        return checked

    def _fetch(self, broker_order_id: str) -> AlpacaOrder | None:
        try:
            return self._broker.get_order(broker_order_id)
        except BrokerError as exc:
            logger.warning("reconcile get_order failed id=%s: %s", broker_order_id, exc)
            return None

    def _apply_entry(self, position: Position, order: AlpacaOrder) -> None:
        status = order.status.lower()
        if status in _ALPACA_FILLED:
            self._writer.update(
                position.trade_id,
                status=TradeStatus.FILLED.value,
                filled_quantity=order.filled_qty,
                filled_avg_price=order.filled_avg_price,
                filled_at=datetime.now(UTC),
            )
            if order.filled_qty is not None:
                position.quantity = order.filled_qty
            if order.filled_avg_price is not None:
                position.entry_price = order.filled_avg_price
            self._book.mark_open(position)
            self._capture_hold(position)
        elif status in _ALPACA_PARTIAL:
            self._writer.update(
                position.trade_id,
                status=TradeStatus.PARTIALLY_FILLED.value,
                filled_quantity=order.filled_qty,
                filled_avg_price=order.filled_avg_price,
            )
        elif status in _ALPACA_DEAD:
            self._writer.update(position.trade_id, status=TradeStatus.REJECTED.value)
            self._book.remove(position)
            self._release_hold(position)

    def _apply_exit(self, position: Position, order: AlpacaOrder) -> None:
        status = order.status.lower()
        if status in _ALPACA_FILLED:
            self._writer.update(
                position.trade_id,
                status=TradeStatus.CLOSED.value,
                exit_filled_quantity=order.filled_qty,
                exit_filled_avg_price=order.filled_avg_price,
                closed_at=datetime.now(UTC),
            )
            self._book.remove(position)
            if self._on_close is not None:
                self._on_close(position.idempotency_key)
        elif status in _ALPACA_PARTIAL:
            self._writer.update(
                position.trade_id,
                status=TradeStatus.EXIT_PARTIALLY_FILLED.value,
                exit_filled_quantity=order.filled_qty,
                exit_filled_avg_price=order.filled_avg_price,
            )
        elif status in _ALPACA_DEAD:
            # Exit was rejected; reopen so it is retried on the next trigger or at market close.
            self._writer.update(position.trade_id, status=TradeStatus.EXIT_FAILED.value)
            self._book.release_exit(position)

    def _capture_hold(self, position: Position) -> None:
        if position.cash_hold_id is None:
            return
        try:
            self._cash.capture_hold(position.cash_hold_id)
        except CashServiceError as exc:
            logger.warning("failed to capture hold %s: %s", position.cash_hold_id, exc)

    def _release_hold(self, position: Position) -> None:
        if position.cash_hold_id is None:
            return
        try:
            self._cash.release_hold(position.cash_hold_id)
        except CashServiceError as exc:
            logger.warning("failed to release hold %s: %s", position.cash_hold_id, exc)

    def run_forever(self, stop_event: threading.Event, interval_seconds: float) -> None:
        """Reconcile every ``interval_seconds`` until ``stop_event`` is set."""
        while not stop_event.is_set():
            try:
                self.reconcile_once()
            except Exception:
                logger.exception("reconciliation pass failed; will retry")
            stop_event.wait(interval_seconds)


class MarketCloseLiquidator:
    """Flattens all open positions once per day at the configured close time (SPEC.md §5.7)."""

    def __init__(
        self,
        service: ExecutionService,
        close_time: clock_time | None,
        tz: ZoneInfo,
        *,
        check_seconds: float = 30.0,
    ) -> None:
        self._service = service
        self._close_time = close_time
        self._tz = tz
        self._check_seconds = check_seconds
        self._last_run: date | None = None

    def maybe_liquidate(self, now: datetime | None = None) -> int:
        """Liquidate if we are at/after the close time and have not already done so today."""
        if self._close_time is None:
            return 0
        moment = now or datetime.now(self._tz)
        today = moment.date()
        if moment.time() >= self._close_time and self._last_run != today:
            self._last_run = today
            count = self._service.liquidate_all(ExitReason.MARKET_CLOSE)
            logger.info(
                format_event(
                    "market_close_liquidation", positions=count, close=str(self._close_time)
                )
            )
            return count
        return 0

    def run_forever(self, stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            try:
                self.maybe_liquidate()
            except Exception:
                logger.exception("market close check failed; will retry")
            stop_event.wait(self._check_seconds)
