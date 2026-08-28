"""create trades table

Revision ID: 0001_create_trades
Revises:
Create Date: 2025-01-01 00:00:00.000000

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0001_create_trades"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "trades",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("execution_mode", sa.String(length=8), nullable=False),
        sa.Column("is_paper", sa.Boolean(), nullable=False),
        sa.Column("symbol", sa.String(length=16), nullable=False),
        sa.Column("side", sa.String(length=8), nullable=False),
        sa.Column("position_type", sa.String(length=8), nullable=False),
        sa.Column("target_buy_price", sa.Numeric(20, 8), nullable=False),
        sa.Column("trigger_price", sa.Numeric(20, 8), nullable=False),
        sa.Column("quantity", sa.Numeric(20, 8), nullable=False),
        sa.Column("notional", sa.Numeric(20, 8), nullable=False),
        sa.Column("currency", sa.CHAR(length=3), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("broker", sa.String(length=16), nullable=False),
        sa.Column("broker_order_id", sa.String(length=64), nullable=True),
        sa.Column("filled_quantity", sa.Numeric(20, 8), nullable=True),
        sa.Column("filled_avg_price", sa.Numeric(20, 8), nullable=True),
        sa.Column("cash_hold_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("source_query_id", sa.String(length=64), nullable=True),
        sa.Column("trigger_reason", sa.String(length=255), nullable=True),
        sa.Column("idempotency_key", sa.String(length=200), nullable=False),
        sa.Column("kafka_topic", sa.String(length=128), nullable=True),
        sa.Column("kafka_partition", sa.Integer(), nullable=True),
        sa.Column("kafka_offset", sa.BigInteger(), nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("filled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_trades_symbol", "trades", ["symbol"])
    op.create_index("idx_trades_status", "trades", ["status"])
    op.create_index("idx_trades_is_paper", "trades", ["is_paper"])
    op.create_index("idx_trades_created_at", "trades", ["created_at"])
    op.create_index(
        "uq_trades_idempotency_key", "trades", ["idempotency_key"], unique=True
    )


def downgrade() -> None:
    op.drop_index("uq_trades_idempotency_key", table_name="trades")
    op.drop_index("idx_trades_created_at", table_name="trades")
    op.drop_index("idx_trades_is_paper", table_name="trades")
    op.drop_index("idx_trades_status", table_name="trades")
    op.drop_index("idx_trades_symbol", table_name="trades")
    op.drop_table("trades")
