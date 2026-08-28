"""Migration + repository smoke test.

Requires a reachable Postgres (``DATABASE_URL``). Runs fully in CI; skips locally when no
database is reachable.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError, OperationalError

from quant_execution.db import get_engine, session_scope
from quant_execution.repository.models import Trade
from quant_execution.repository.trades_repo import TradesRepository


def _database_reachable() -> bool:
    try:
        with get_engine().connect():
            return True
    except OperationalError:
        return False


pytestmark = pytest.mark.skipif(
    not _database_reachable(), reason="no Postgres reachable"
)


def _make_trade(idempotency_key: str) -> Trade:
    return Trade(
        execution_mode="paper",
        is_paper=True,
        symbol="AAPL",
        side="BUY",
        position_type="LONG",
        target_buy_price=Decimal("100.00000000"),
        trigger_price=Decimal("99.50000000"),
        quantity=Decimal("10.00000000"),
        notional=Decimal("995.00000000"),
        currency="USD",
        status="FILLED",
        broker="alpaca",
        idempotency_key=idempotency_key,
    )


def test_trades_table_exists_with_indexes() -> None:
    inspector = inspect(get_engine())
    assert "trades" in inspector.get_table_names()
    index_names = {ix["name"] for ix in inspector.get_indexes("trades")}
    assert {
        "idx_trades_symbol",
        "idx_trades_status",
        "idx_trades_is_paper",
        "idx_trades_created_at",
        "uq_trades_idempotency_key",
    } <= index_names


def test_repository_round_trip() -> None:
    key = f"paper:AAPL:test:{uuid.uuid4()}"
    with session_scope() as session:
        repo = TradesRepository(session)
        assert repo.exists_by_idempotency_key(key) is False
        trade = repo.insert(_make_trade(key))
        trade_id = trade.id

    with session_scope() as session:
        repo = TradesRepository(session)
        assert repo.exists_by_idempotency_key(key) is True
        fetched = repo.get(trade_id)
        assert fetched is not None
        assert fetched.symbol == "AAPL"
        assert fetched.quantity == Decimal("10.00000000")


def test_duplicate_idempotency_key_rejected() -> None:
    key = f"paper:AAPL:dup:{uuid.uuid4()}"
    with session_scope() as session:
        TradesRepository(session).insert(_make_trade(key))

    with pytest.raises(IntegrityError), session_scope() as session:
        TradesRepository(session).insert(_make_trade(key))
