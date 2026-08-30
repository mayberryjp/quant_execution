"""Tests for daily P&L computation, the reporter scheduler, and the repository."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime
from datetime import time as clock_time
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

from quant_execution.domain.enums import ExecutionMode
from quant_execution.domain.pnl import (
    DailyPnlReporter,
    DailyPnlStats,
    compute_daily_pnl,
)
from quant_execution.repository.models import Trade
from quant_execution.repository.pnl_repo import DailyPnlRepository

_NY = ZoneInfo("America/New_York")


def _closed_trade(
    *,
    symbol: str,
    position_type: str = "LONG",
    entry: str,
    exit_price: str | None,
    qty: str = "1",
    is_paper: bool = True,
) -> Trade:
    trade = Trade(
        id=uuid.uuid4(),
        execution_mode="paper" if is_paper else "live",
        is_paper=is_paper,
        symbol=symbol,
        side="BUY",
        position_type=position_type,
        target_buy_price=Decimal(entry),
        trigger_price=Decimal(entry),
        quantity=Decimal(qty),
        notional=Decimal(entry) * Decimal(qty),
        status="CLOSED",
        broker="paper",
        idempotency_key=f"paper:{symbol}:x:2026-08-30",
        filled_avg_price=Decimal(entry),
        filled_quantity=Decimal(qty),
        exit_filled_avg_price=Decimal(exit_price) if exit_price is not None else None,
        exit_filled_quantity=Decimal(qty) if exit_price is not None else None,
    )
    return trade


def test_compute_daily_pnl_aggregates() -> None:
    trades = [
        _closed_trade(symbol="AAA", entry="100", exit_price="110", qty="2"),  # +20 win
        _closed_trade(symbol="BBB", entry="100", exit_price="90", qty="1"),  # -10 loss
        _closed_trade(
            symbol="CCC", position_type="SHORT", entry="50", exit_price="40", qty="3"
        ),  # +30 win
        _closed_trade(symbol="DDD", entry="20", exit_price="20", qty="5"),  # breakeven
        _closed_trade(symbol="EEE", entry="10", exit_price=None),  # skipped (no exit)
    ]

    stats = compute_daily_pnl(
        trades, trade_date=date(2026, 8, 30), mode=ExecutionMode.PAPER
    )

    assert stats.total_trades == 4
    assert stats.winning_trades == 2
    assert stats.losing_trades == 1
    assert stats.breakeven_trades == 1
    assert stats.symbols_traded == 4
    assert stats.realized_pnl == Decimal(40)
    assert stats.amount_invested == Decimal(550)
    assert stats.gross_proceeds == Decimal(530)
    assert stats.win_rate == Decimal(50)
    assert stats.average_win == Decimal(25)
    assert stats.average_loss == Decimal(-10)
    assert stats.largest_win == Decimal(30)
    assert stats.largest_loss == Decimal(-10)
    assert stats.is_paper is True
    assert stats.execution_mode == "paper"


def test_compute_daily_pnl_empty() -> None:
    stats = compute_daily_pnl([], trade_date=date(2026, 8, 30), mode=ExecutionMode.LIVE)
    assert stats.total_trades == 0
    assert stats.realized_pnl == Decimal(0)
    assert stats.win_rate == Decimal(0)
    assert stats.return_pct == Decimal(0)
    assert stats.is_paper is False


class _FakeTradesRepo:
    def __init__(self, trades: list[Trade]) -> None:
        self._trades = trades
        self.calls: list[tuple[datetime, datetime, bool]] = []

    def list_closed_between(
        self, start: datetime, end: datetime, *, is_paper: bool, limit: int = 100_000
    ) -> list[Trade]:
        self.calls.append((start, end, is_paper))
        return [t for t in self._trades if t.is_paper == is_paper]


class _FakePnlRepo:
    def __init__(self) -> None:
        self.upserts: list[DailyPnlStats] = []

    def upsert(self, stats: DailyPnlStats) -> DailyPnlStats:
        self.upserts.append(stats)
        return stats


def _reporter(trades: list[Trade]) -> tuple[DailyPnlReporter, _FakeTradesRepo, _FakePnlRepo]:
    trades_repo = _FakeTradesRepo(trades)
    pnl_repo = _FakePnlRepo()

    @contextmanager
    def _uow() -> Iterator[tuple[Any, Any]]:
        yield trades_repo, pnl_repo

    schedule = [
        (ExecutionMode.PAPER, clock_time(15, 0)),
        (ExecutionMode.LIVE, None),
    ]
    reporter = DailyPnlReporter(schedule, _NY, unit_of_work=_uow, check_seconds=1.0)
    return reporter, trades_repo, pnl_repo


def test_reporter_fires_once_after_close() -> None:
    trades = [_closed_trade(symbol="AAA", entry="10", exit_price="12", qty="1")]
    reporter, trades_repo, pnl_repo = _reporter(trades)

    before = datetime(2026, 8, 30, 14, 0, tzinfo=_NY)
    assert reporter.maybe_report(now=before) == []
    assert pnl_repo.upserts == []

    at_close = datetime(2026, 8, 30, 15, 0, tzinfo=_NY)
    reported = reporter.maybe_report(now=at_close)
    assert len(reported) == 1  # live is disabled (None close time)
    stats = reported[0]
    assert stats.is_paper is True
    assert stats.total_trades == 1
    assert stats.realized_pnl == Decimal(2)
    assert len(pnl_repo.upserts) == 1

    # Window starts at the previous day's close.
    start, _end, is_paper = trades_repo.calls[0]
    assert start == datetime(2026, 8, 29, 15, 0, tzinfo=_NY)
    assert is_paper is True

    # A later check the same day does not re-report.
    later = datetime(2026, 8, 30, 16, 0, tzinfo=_NY)
    assert reporter.maybe_report(now=later) == []
    assert len(pnl_repo.upserts) == 1


def _sample_stats() -> DailyPnlStats:
    return DailyPnlStats(
        trade_date=date(2026, 8, 30),
        execution_mode="paper",
        is_paper=True,
        total_trades=2,
        winning_trades=1,
        losing_trades=1,
        breakeven_trades=0,
        symbols_traded=2,
        realized_pnl=Decimal(5),
        amount_invested=Decimal(200),
        gross_proceeds=Decimal(205),
        win_rate=Decimal(50),
        return_pct=Decimal("2.5"),
        average_win=Decimal(10),
        average_loss=Decimal(-5),
        largest_win=Decimal(10),
        largest_loss=Decimal(-5),
    )


def test_repository_upsert_inserts_when_absent() -> None:
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None
    repo = DailyPnlRepository(session)

    repo.upsert(_sample_stats())

    session.add.assert_called_once()
    session.flush.assert_called_once()


def test_repository_upsert_updates_existing() -> None:
    from quant_execution.repository.models import DailyPnl

    existing = DailyPnl()
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = existing
    repo = DailyPnlRepository(session)

    repo.upsert(_sample_stats())

    session.add.assert_not_called()
    assert existing.realized_pnl == Decimal(5)
    assert existing.total_trades == 2


def test_repository_list_history() -> None:
    from quant_execution.repository.models import DailyPnl

    rows = [DailyPnl(), DailyPnl()]
    session = MagicMock()
    session.execute.return_value.scalars.return_value.all.return_value = rows
    repo = DailyPnlRepository(session)

    assert repo.list_history(is_paper=True, limit=10, offset=0) == rows
