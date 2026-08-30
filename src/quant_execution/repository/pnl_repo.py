"""Data-access helpers for the ``daily_pnl`` table.

Access goes through the SQLAlchemy ORM / Core, which parameterizes values; raw string SQL
interpolation is never used.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.orm import Session

from quant_execution.repository.models import DailyPnl

if TYPE_CHECKING:
    from quant_execution.domain.pnl import DailyPnlStats

__all__ = ["DailyPnlRepository"]


def _stats_to_values(stats: DailyPnlStats) -> dict[str, object]:
    return {
        "trade_date": stats.trade_date,
        "execution_mode": stats.execution_mode,
        "is_paper": stats.is_paper,
        "total_trades": stats.total_trades,
        "winning_trades": stats.winning_trades,
        "losing_trades": stats.losing_trades,
        "breakeven_trades": stats.breakeven_trades,
        "symbols_traded": stats.symbols_traded,
        "realized_pnl": stats.realized_pnl,
        "amount_invested": stats.amount_invested,
        "gross_proceeds": stats.gross_proceeds,
        "win_rate": stats.win_rate,
        "return_pct": stats.return_pct,
        "average_win": stats.average_win,
        "average_loss": stats.average_loss,
        "largest_win": stats.largest_win,
        "largest_loss": stats.largest_loss,
    }


class DailyPnlRepository:
    """Repository over a single SQLAlchemy :class:`Session`."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def get_for_date(self, trade_date: date, *, is_paper: bool) -> DailyPnl | None:
        stmt = (
            select(DailyPnl)
            .where(DailyPnl.trade_date == trade_date, DailyPnl.is_paper == is_paper)
            .limit(1)
        )
        return self._session.execute(stmt).scalar_one_or_none()

    def upsert(self, stats: DailyPnlStats) -> DailyPnl:
        """Insert a new daily row, or update the existing one for the same date and mode."""
        values = _stats_to_values(stats)
        existing = self.get_for_date(stats.trade_date, is_paper=stats.is_paper)
        if existing is None:
            row = DailyPnl(**values)
            self._session.add(row)
            self._session.flush()
            return row
        for name, value in values.items():
            setattr(existing, name, value)
        self._session.flush()
        return existing

    def list_for_date(self, trade_date: date) -> list[DailyPnl]:
        stmt = (
            select(DailyPnl)
            .where(DailyPnl.trade_date == trade_date)
            .order_by(DailyPnl.is_paper.desc())
        )
        return list(self._session.execute(stmt).scalars().all())

    def list_history(
        self, *, is_paper: bool | None = None, limit: int = 100, offset: int = 0
    ) -> list[DailyPnl]:
        """Most-recent-first daily P&L history with an optional mode filter."""
        stmt = select(DailyPnl)
        if is_paper is not None:
            stmt = stmt.where(DailyPnl.is_paper == is_paper)
        stmt = (
            stmt.order_by(DailyPnl.trade_date.desc(), DailyPnl.is_paper.desc())
            .limit(limit)
            .offset(offset)
        )
        return list(self._session.execute(stmt).scalars().all())
