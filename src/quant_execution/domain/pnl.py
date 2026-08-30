"""End-of-day profit-and-loss reporting.

After each mode's market close the reporter aggregates every trade that closed during the session
into a single :class:`~quant_execution.repository.models.DailyPnl` row (paper and live are recorded
separately). The scheduler mirrors :class:`MarketCloseLiquidator`: it wakes on a fixed cadence and
fires once per day per mode at or after the configured close time, by which point the liquidator has
already flattened any open positions.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from datetime import time as clock_time
from decimal import Decimal
from zoneinfo import ZoneInfo

from quant_execution.db import session_scope
from quant_execution.domain.enums import ExecutionMode, PositionType
from quant_execution.logging import format_event, get_logger
from quant_execution.repository.models import Trade
from quant_execution.repository.pnl_repo import DailyPnlRepository
from quant_execution.repository.trades_repo import TradesRepository

__all__ = ["DailyPnlReporter", "DailyPnlStats", "PnlUnitOfWork", "compute_daily_pnl"]

logger = get_logger(__name__)

_ZERO = Decimal(0)
_QUANT = Decimal("0.00000001")


def _q(value: Decimal) -> Decimal:
    """Quantize to the 8 decimal places stored by the ``Numeric(20, 8)`` P&L columns."""
    return value.quantize(_QUANT)


@dataclass(frozen=True)
class DailyPnlStats:
    """Aggregated realized P&L for one trading day and mode."""

    trade_date: date
    execution_mode: str
    is_paper: bool
    total_trades: int
    winning_trades: int
    losing_trades: int
    breakeven_trades: int
    symbols_traded: int
    realized_pnl: Decimal
    amount_invested: Decimal
    gross_proceeds: Decimal
    win_rate: Decimal
    return_pct: Decimal
    average_win: Decimal
    average_loss: Decimal
    largest_win: Decimal
    largest_loss: Decimal


def _trade_pnl(trade: Trade) -> Decimal | None:
    """Realized P&L for one closed trade, or ``None`` if entry/exit fills are missing."""
    entry_price = trade.filled_avg_price
    exit_price = trade.exit_filled_avg_price
    quantity = trade.exit_filled_quantity or trade.filled_quantity
    if entry_price is None or exit_price is None or quantity is None:
        return None
    if trade.position_type == PositionType.LONG.value:
        return (exit_price - entry_price) * quantity
    return (entry_price - exit_price) * quantity


def compute_daily_pnl(
    trades: Iterable[Trade], *, trade_date: date, mode: ExecutionMode
) -> DailyPnlStats:
    """Aggregate realized P&L over the closed ``trades`` of a single session."""
    total = wins = losses = breakeven = 0
    realized = invested = proceeds = _ZERO
    win_sum = loss_sum = largest_win = largest_loss = _ZERO
    symbols: set[str] = set()

    for trade in trades:
        pnl = _trade_pnl(trade)
        if pnl is None:
            continue
        total += 1
        symbols.add(trade.symbol)

        entry_price = trade.filled_avg_price or _ZERO
        entry_qty = trade.filled_quantity or trade.exit_filled_quantity or _ZERO
        exit_qty = trade.exit_filled_quantity or entry_qty
        invested += entry_price * entry_qty
        proceeds += (trade.exit_filled_avg_price or _ZERO) * exit_qty
        realized += pnl

        if pnl > 0:
            wins += 1
            win_sum += pnl
            largest_win = max(largest_win, pnl)
        elif pnl < 0:
            losses += 1
            loss_sum += pnl
            largest_loss = min(largest_loss, pnl)
        else:
            breakeven += 1

    win_rate = (Decimal(wins) / Decimal(total) * 100) if total else _ZERO
    return_pct = (realized / invested * 100) if invested > 0 else _ZERO
    average_win = (win_sum / Decimal(wins)) if wins else _ZERO
    average_loss = (loss_sum / Decimal(losses)) if losses else _ZERO

    return DailyPnlStats(
        trade_date=trade_date,
        execution_mode=mode.value,
        is_paper=mode.is_paper,
        total_trades=total,
        winning_trades=wins,
        losing_trades=losses,
        breakeven_trades=breakeven,
        symbols_traded=len(symbols),
        realized_pnl=_q(realized),
        amount_invested=_q(invested),
        gross_proceeds=_q(proceeds),
        win_rate=_q(win_rate),
        return_pct=_q(return_pct),
        average_win=_q(average_win),
        average_loss=_q(average_loss),
        largest_win=_q(largest_win),
        largest_loss=_q(largest_loss),
    )


PnlUnitOfWork = Callable[
    [], AbstractContextManager[tuple[TradesRepository, DailyPnlRepository]]
]


@contextmanager
def _default_pnl_unit_of_work() -> Iterator[tuple[TradesRepository, DailyPnlRepository]]:
    with session_scope() as session:
        yield TradesRepository(session), DailyPnlRepository(session)


class DailyPnlReporter:
    """Writes a daily P&L row once per day per mode at or after that mode's close time."""

    def __init__(
        self,
        schedule: Sequence[tuple[ExecutionMode, clock_time | None]],
        tz: ZoneInfo,
        *,
        unit_of_work: PnlUnitOfWork = _default_pnl_unit_of_work,
        check_seconds: float = 900.0,
    ) -> None:
        self._schedule = list(schedule)
        self._tz = tz
        self._unit_of_work = unit_of_work
        self._check_seconds = check_seconds
        self._last_run: dict[ExecutionMode, date] = {}

    def maybe_report(self, now: datetime | None = None) -> list[DailyPnlStats]:
        """Report any mode that has reached its close time and not yet run today."""
        moment = now or datetime.now(self._tz)
        results: list[DailyPnlStats] = []
        for mode, close_time in self._schedule:
            stats = self._maybe_report_mode(mode, close_time, moment)
            if stats is not None:
                results.append(stats)
        return results

    def _maybe_report_mode(
        self, mode: ExecutionMode, close_time: clock_time | None, moment: datetime
    ) -> DailyPnlStats | None:
        if close_time is None:
            return None
        today = moment.date()
        if moment.time() < close_time or self._last_run.get(mode) == today:
            return None
        self._last_run[mode] = today

        close_dt = datetime.combine(today, close_time, tzinfo=self._tz)
        window_start = close_dt - timedelta(days=1)
        with self._unit_of_work() as (trades_repo, pnl_repo):
            trades = trades_repo.list_closed_between(
                window_start, moment, is_paper=mode.is_paper
            )
            stats = compute_daily_pnl(trades, trade_date=today, mode=mode)
            pnl_repo.upsert(stats)
        logger.info(
            format_event(
                "daily_pnl_reported",
                mode=mode.value,
                trade_date=today.isoformat(),
                trades=stats.total_trades,
                realized_pnl=stats.realized_pnl,
                win_rate=stats.win_rate,
            )
        )
        return stats

    def run_forever(self, stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            try:
                self.maybe_report()
            except Exception:
                logger.exception("daily pnl report failed; will retry")
            stop_event.wait(self._check_seconds)
