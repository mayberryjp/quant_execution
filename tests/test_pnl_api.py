"""Tests for the /pnl daily P&L endpoints."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import patch

from webtest import TestApp

from quant_execution.repository.models import DailyPnl


@contextmanager
def _fake_scope() -> Iterator[None]:
    yield None


def _make_pnl(**overrides: Any) -> DailyPnl:
    row = DailyPnl(
        id=uuid.uuid4(),
        trade_date=date(2026, 8, 30),
        execution_mode="paper",
        is_paper=True,
        total_trades=3,
        winning_trades=2,
        losing_trades=1,
        breakeven_trades=0,
        symbols_traded=3,
        realized_pnl=Decimal("42.50000000"),
        amount_invested=Decimal("1000.00000000"),
        gross_proceeds=Decimal("1042.50000000"),
        win_rate=Decimal("66.66666667"),
        return_pct=Decimal("4.25000000"),
        average_win=Decimal("30.00000000"),
        average_loss=Decimal("-17.50000000"),
        largest_win=Decimal("35.00000000"),
        largest_loss=Decimal("-17.50000000"),
        currency="USD",
        created_at=datetime(2026, 8, 30, 20, tzinfo=UTC),
        updated_at=datetime(2026, 8, 30, 20, tzinfo=UTC),
    )
    for key, value in overrides.items():
        setattr(row, key, value)
    return row


def test_list_pnl_ok(client: TestApp) -> None:
    rows = [_make_pnl(), _make_pnl(execution_mode="live", is_paper=False)]
    with (
        patch("quant_execution.api.routes.pnl.session_scope", _fake_scope),
        patch("quant_execution.api.routes.pnl.DailyPnlRepository") as repo,
    ):
        repo.return_value.list_history.return_value = rows
        resp = client.get("/pnl")

    assert resp.status_code == 200
    assert resp.json["status"] == "ok"
    assert resp.json["count"] == 2
    # Decimals serialize as strings to preserve precision.
    assert resp.json["pnl"][0]["realized_pnl"] == "42.50000000"
    assert resp.json["pnl"][0]["trade_date"] == "2026-08-30"


def test_list_pnl_applies_mode_filter(client: TestApp) -> None:
    with (
        patch("quant_execution.api.routes.pnl.session_scope", _fake_scope),
        patch("quant_execution.api.routes.pnl.DailyPnlRepository") as repo,
    ):
        repo.return_value.list_history.return_value = []
        resp = client.get("/pnl?mode=live&limit=10&offset=2")

    assert resp.status_code == 200
    repo.return_value.list_history.assert_called_once_with(is_paper=False, limit=10, offset=2)


def test_list_pnl_invalid_mode(client: TestApp) -> None:
    resp = client.get("/pnl?mode=demo", expect_errors=True)
    assert resp.status_code == 400
    assert resp.json["code"] == "invalid_mode"


def test_list_pnl_invalid_pagination(client: TestApp) -> None:
    resp = client.get("/pnl?limit=abc", expect_errors=True)
    assert resp.status_code == 400
    assert resp.json["code"] == "invalid_pagination"


def test_get_pnl_by_date_ok(client: TestApp) -> None:
    rows = [_make_pnl(), _make_pnl(execution_mode="live", is_paper=False)]
    with (
        patch("quant_execution.api.routes.pnl.session_scope", _fake_scope),
        patch("quant_execution.api.routes.pnl.DailyPnlRepository") as repo,
    ):
        repo.return_value.list_for_date.return_value = rows
        resp = client.get("/pnl/2026-08-30")

    assert resp.status_code == 200
    assert resp.json["count"] == 2
    assert resp.json["trade_date"] == "2026-08-30"


def test_get_pnl_by_date_not_found(client: TestApp) -> None:
    with (
        patch("quant_execution.api.routes.pnl.session_scope", _fake_scope),
        patch("quant_execution.api.routes.pnl.DailyPnlRepository") as repo,
    ):
        repo.return_value.list_for_date.return_value = []
        resp = client.get("/pnl/2026-08-30", expect_errors=True)

    assert resp.status_code == 404
    assert resp.json["code"] == "not_found"


def test_get_pnl_invalid_date(client: TestApp) -> None:
    resp = client.get("/pnl/not-a-date", expect_errors=True)
    assert resp.status_code == 400
    assert resp.json["code"] == "invalid_date"
