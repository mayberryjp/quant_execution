"""add exit/sell columns to trades

Adds the sell-side lifecycle columns so a position's entry row is updated in place when it is
closed (target price hit or market-close liquidation); see SPEC.md §5.7. Columns are nullable so
the migration is safe against any pre-existing rows.

Revision ID: 0002_add_exit_columns
Revises: 0001_create_trades
Create Date: 2026-08-28 00:00:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0002_add_exit_columns"
down_revision: str | None = "0001_create_trades"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("trades", sa.Column("target_sell_price", sa.Numeric(20, 8), nullable=True))
    op.add_column("trades", sa.Column("exit_broker_order_id", sa.String(length=64), nullable=True))
    op.add_column("trades", sa.Column("exit_filled_quantity", sa.Numeric(20, 8), nullable=True))
    op.add_column("trades", sa.Column("exit_filled_avg_price", sa.Numeric(20, 8), nullable=True))
    op.add_column("trades", sa.Column("exit_reason", sa.String(length=16), nullable=True))
    op.add_column("trades", sa.Column("exit_submitted_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("trades", sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("trades", "closed_at")
    op.drop_column("trades", "exit_submitted_at")
    op.drop_column("trades", "exit_reason")
    op.drop_column("trades", "exit_filled_avg_price")
    op.drop_column("trades", "exit_filled_quantity")
    op.drop_column("trades", "exit_broker_order_id")
    op.drop_column("trades", "target_sell_price")
