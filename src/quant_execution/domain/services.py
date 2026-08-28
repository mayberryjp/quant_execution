"""Execution core: turn a matched (tick, entry) into a persisted trade (SPEC.md §5.5).

The service is mode-agnostic; the two modes share one code path and differ only in the four
documented ways (topic, ``is_paper``, live cash check, and fill confirmation). Sizing (§5.3) and
idempotency (§5.4) are shared. The live path also runs a background reconciliation loop (§5.6).
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from typing import Protocol

from quant_execution.clients.alpaca_client import AlpacaBroker, AlpacaOrder
from quant_execution.clients.cash_client import CashClient
from quant_execution.db import session_scope
from quant_execution.domain.enums import ExecutionMode, TradeStatus
from quant_execution.domain.exceptions import BrokerError, CashServiceError, ConfigurationError
from quant_execution.domain.schemas import Tick, WatchlistEntry
from quant_execution.kafka.consumer import MessageMeta
from quant_execution.logging import format_event, get_logger
from quant_execution.repository.models import Trade
from quant_execution.repository.trades_repo import TradesRepository

logger = get_logger(__name__)

# Alpaca order statuses mapped to terminal/non-terminal outcomes (SPEC.md §5.6).
_ALPACA_FILLED = frozenset({"filled"})
_ALPACA_PARTIAL = frozenset({"partially_filled"})
_ALPACA_DEAD = frozenset({"canceled", "cancelled", "rejected", "expired", "done_for_day"})
_RECONCILE_BATCH = 100



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


class _TradesRepo(Protocol):
    def exists_by_idempotency_key(self, idempotency_key: str) -> bool: ...
    def insert(self, trade: Trade) -> Trade: ...


UnitOfWork = Callable[[], AbstractContextManager[_TradesRepo]]
ReconcileUnitOfWork = Callable[[], AbstractContextManager[TradesRepository]]


@contextmanager
def _default_unit_of_work() -> Iterator[_TradesRepo]:
    with session_scope() as session:
        yield TradesRepository(session)


@contextmanager
def _default_reconcile_unit_of_work() -> Iterator[TradesRepository]:
    with session_scope() as session:
        yield TradesRepository(session)


class ExecutionService:
    """Executes matched signals into ``trades`` rows (one transaction per trade)."""

    def __init__(
        self,
        mode: ExecutionMode,
        broker: AlpacaBroker,
        *,
        notional_usd: float | None,
        quantity: float | None,
        cash: CashClient | None = None,
        unit_of_work: UnitOfWork = _default_unit_of_work,
    ) -> None:
        if not mode.is_paper and cash is None:
            raise ConfigurationError("live mode requires a cash client")
        self._mode = mode
        self._broker = broker
        self._notional_usd = notional_usd
        self._quantity = quantity
        self._cash = cash
        self._unit_of_work = unit_of_work

    def execute(
        self, tick: Tick, entry: WatchlistEntry, *, provenance: MessageMeta
    ) -> Trade | None:
        """Execute one matched signal; returns the trade, or ``None`` if it was a dedup no-op."""
        start = time.perf_counter()
        key = build_idempotency_key(self._mode, entry, tick)
        qty, notional = compute_sizing(
            tick.price, notional_usd=self._notional_usd, quantity=self._quantity
        )

        with self._unit_of_work() as repo:
            if repo.exists_by_idempotency_key(key):
                logger.info(format_event("trade_dedup", symbol=entry.symbol, idempotency_key=key))
                return None


            trade = Trade(
                execution_mode=self._mode.value,
                is_paper=self._mode.is_paper,
                symbol=entry.symbol,
                side=entry.position_type.entry_side.value,
                position_type=entry.position_type.value,
                target_buy_price=entry.buy_price,
                trigger_price=tick.price,
                quantity=qty,
                notional=notional,
                status=TradeStatus.NEW.value,
                broker=self._broker.name,
                idempotency_key=key,
                source_query_id=entry.source_query_id,
                trigger_reason=entry.trigger_reason,
                kafka_topic=provenance.topic,
                kafka_partition=provenance.partition,
                kafka_offset=provenance.offset,
            )
            repo.insert(trade)

            if not self._mode.is_paper and not self._reserve_cash(trade, notional, key):
                self._log_result(trade, key, start)
                return trade

            try:
                order = self._broker.submit_order(
                    symbol=entry.symbol,
                    qty=qty,
                    side=entry.position_type.entry_side,
                    client_order_id=key,
                )
            except BrokerError as exc:
                trade.status = TradeStatus.FAILED.value
                trade.error_code = "BROKER_ERROR"
                trade.error_detail = str(exc)
                logger.warning("broker submit failed key=%s: %s", key, exc)
                self._release_hold_safe(trade)
                self._log_result(trade, key, start)
                return trade

            now = datetime.now(UTC)
            trade.broker_order_id = order.id
            trade.status = TradeStatus.SUBMITTED.value
            trade.submitted_at = now

            if self._mode.is_paper:
                # Paper fills are assumed immediately; live fills are confirmed by §5.6.
                trade.status = TradeStatus.FILLED.value
                trade.filled_quantity = qty
                trade.filled_avg_price = tick.price
                trade.filled_at = now

            self._log_result(trade, key, start)
            return trade

    def _log_result(self, trade: Trade, key: str, start: float) -> None:
        duration_ms = round((time.perf_counter() - start) * 1000, 2)
        logger.info(
            format_event(
                "trade_executed",
                symbol=trade.symbol,
                mode=self._mode.value,
                idempotency_key=key,
                broker_order_id=trade.broker_order_id,
                status=trade.status,
                duration_ms=duration_ms,
            )
        )


    def _reserve_cash(self, trade: Trade, notional: Decimal, key: str) -> bool:
        """Live-only: check balance and place a hold. Returns False if the trade is terminal."""
        cash = self._cash
        if cash is None:  # pragma: no cover - guarded at construction
            raise ConfigurationError("live mode requires a cash client")
        try:
            balance = cash.get_available_balance()
            if balance < notional:
                trade.status = TradeStatus.INSUFFICIENT_FUNDS.value
                logger.info("insufficient funds key=%s balance=%s notional=%s", key, balance, notional)
                return False
            hold = cash.place_hold(notional, reason=key, reference_id=key)
        except CashServiceError as exc:
            trade.status = TradeStatus.FAILED.value
            trade.error_code = "CASH_ERROR"
            trade.error_detail = str(exc)
            logger.warning("cash reservation failed key=%s: %s", key, exc)
            return False
        trade.cash_hold_id = hold.id
        trade.status = TradeStatus.CASH_HELD.value
        return True

    def _release_hold_safe(self, trade: Trade) -> None:
        if self._mode.is_paper or self._cash is None or trade.cash_hold_id is None:
            return
        try:
            self._cash.release_hold(trade.cash_hold_id)
        except CashServiceError as exc:
            logger.warning("failed to release hold %s: %s", trade.cash_hold_id, exc)


class ReconciliationService:
    """Live-only background loop that advances SUBMITTED/PARTIALLY_FILLED trades (SPEC.md §5.6)."""

    def __init__(
        self,
        broker: AlpacaBroker,
        cash: CashClient,
        *,
        batch_size: int = _RECONCILE_BATCH,
        unit_of_work: ReconcileUnitOfWork = _default_reconcile_unit_of_work,
    ) -> None:
        self._broker = broker
        self._cash = cash
        self._batch_size = batch_size
        self._unit_of_work = unit_of_work

    def reconcile_once(self) -> int:
        """Poll open live trades once; return how many were checked."""
        checked = 0
        with self._unit_of_work() as repo:
            open_trades = repo.list_by_status(
                TradeStatus.SUBMITTED.value, limit=self._batch_size
            ) + repo.list_by_status(TradeStatus.PARTIALLY_FILLED.value, limit=self._batch_size)
            for trade in open_trades:
                if trade.broker_order_id is None:
                    continue
                try:
                    order = self._broker.get_order(trade.broker_order_id)
                except BrokerError as exc:
                    logger.warning("reconcile get_order failed id=%s: %s", trade.broker_order_id, exc)
                    continue
                self._apply(repo, trade, order)
                checked += 1
        return checked

    def _apply(self, repo: TradesRepository, trade: Trade, order: AlpacaOrder) -> None:
        status = order.status.lower()
        if status in _ALPACA_FILLED:
            repo.update_status(
                trade,
                TradeStatus.FILLED.value,
                filled_quantity=order.filled_qty,
                filled_avg_price=order.filled_avg_price,
                filled_at=datetime.now(UTC),
            )
            self._capture_hold(trade)
        elif status in _ALPACA_PARTIAL:
            repo.update_status(
                trade,
                TradeStatus.PARTIALLY_FILLED.value,
                filled_quantity=order.filled_qty,
                filled_avg_price=order.filled_avg_price,
            )
        elif status in _ALPACA_DEAD:
            repo.update_status(trade, TradeStatus.REJECTED.value)
            self._release_hold(trade)
        # Non-terminal (new/accepted/pending): leave for the next poll.

    def _capture_hold(self, trade: Trade) -> None:
        if trade.cash_hold_id is None:
            return
        try:
            self._cash.capture_hold(trade.cash_hold_id)
        except CashServiceError as exc:
            logger.warning("failed to capture hold %s: %s", trade.cash_hold_id, exc)

    def _release_hold(self, trade: Trade) -> None:
        if trade.cash_hold_id is None:
            return
        try:
            self._cash.release_hold(trade.cash_hold_id)
        except CashServiceError as exc:
            logger.warning("failed to release hold %s: %s", trade.cash_hold_id, exc)

    def run_forever(self, stop_event: threading.Event, interval_seconds: float) -> None:
        """Reconcile every ``interval_seconds`` until ``stop_event`` is set."""
        while not stop_event.is_set():
            try:
                self.reconcile_once()
            except Exception:
                logger.exception("reconciliation pass failed; will retry")
            stop_event.wait(interval_seconds)

