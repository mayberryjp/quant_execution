"""create daily_pnl table

Adds the end-of-day profit-and-loss summary table. One row per ``(trade_date, is_paper)`` is
written by the daily P&L reporter after each mode's market close.

Revision ID: 0003_create_daily_pnl
Revises: 0002_add_exit_columns
Create Date: 2026-08-30 00:00:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0003_create_daily_pnl"
down_revision: str | None = "0002_add_exit_columns"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "daily_pnl",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("execution_mode", sa.String(length=8), nullable=False),
        sa.Column("is_paper", sa.Boolean(), nullable=False),
        sa.Column("total_trades", sa.Integer(), nullable=False),
        sa.Column("winning_trades", sa.Integer(), nullable=False),
        sa.Column("losing_trades", sa.Integer(), nullable=False),
        sa.Column("breakeven_trades", sa.Integer(), nullable=False),
        sa.Column("symbols_traded", sa.Integer(), nullable=False),
        sa.Column("realized_pnl", sa.Numeric(20, 8), nullable=False),
        sa.Column("amount_invested", sa.Numeric(20, 8), nullable=False),
        sa.Column("gross_proceeds", sa.Numeric(20, 8), nullable=False),
        sa.Column("win_rate", sa.Numeric(20, 8), nullable=False),
        sa.Column("return_pct", sa.Numeric(20, 8), nullable=False),
        sa.Column("average_win", sa.Numeric(20, 8), nullable=False),
        sa.Column("average_loss", sa.Numeric(20, 8), nullable=False),
        sa.Column("largest_win", sa.Numeric(20, 8), nullable=False),
        sa.Column("largest_loss", sa.Numeric(20, 8), nullable=False),
        sa.Column("currency", sa.CHAR(length=3), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("trade_date", "is_paper", name="uq_daily_pnl_date_mode"),
    )
    op.create_index("idx_daily_pnl_trade_date", "daily_pnl", ["trade_date"])
    op.create_index("idx_daily_pnl_is_paper", "daily_pnl", ["is_paper"])


def downgrade() -> None:
    op.drop_index("idx_daily_pnl_is_paper", table_name="daily_pnl")
    op.drop_index("idx_daily_pnl_trade_date", table_name="daily_pnl")
    op.drop_table("daily_pnl")
