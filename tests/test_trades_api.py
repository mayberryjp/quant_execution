"""Tests for the /trades history endpoints."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import patch

from webtest import TestApp

from quant_execution.repository.models import Trade


@contextmanager
def _fake_scope() -> Iterator[None]:
    yield None


def _make_trade(**overrides: Any) -> Trade:
    trade = Trade(
        id=uuid.uuid4(),
        execution_mode="paper",
        is_paper=True,
        symbol="AAPL",
        side="BUY",
        position_type="LONG",
        target_buy_price=Decimal(100),
        trigger_price=Decimal(99),
        quantity=Decimal(2),
        notional=Decimal(198),
        status="FILLED",
        broker="paper",
        idempotency_key="paper:AAPL:dip:2026-08-29",
        created_at=datetime(2026, 8, 29, tzinfo=UTC),
    )
    for key, value in overrides.items():
        setattr(trade, key, value)
    return trade


def test_list_trades_ok(client: TestApp) -> None:
    trades = [_make_trade(symbol="AAPL"), _make_trade(symbol="MSFT")]
    with (
        patch("quant_execution.api.routes.trades.session_scope", _fake_scope),
        patch("quant_execution.api.routes.trades.TradesRepository") as repo,
    ):
        repo.return_value.list_history.return_value = trades
        resp = client.get("/trades")

    assert resp.status_code == 200
    assert resp.json["status"] == "ok"
    assert resp.json["count"] == 2
    assert {t["symbol"] for t in resp.json["trades"]} == {"AAPL", "MSFT"}
    # Decimals serialize as strings to preserve precision.
    assert resp.json["trades"][0]["quantity"] == "2"


def test_list_trades_applies_filters(client: TestApp) -> None:
    with (
        patch("quant_execution.api.routes.trades.session_scope", _fake_scope),
        patch("quant_execution.api.routes.trades.TradesRepository") as repo,
    ):
        repo.return_value.list_history.return_value = []
        resp = client.get("/trades?mode=live&status=CLOSED&symbol=msft&limit=50&offset=5")

    assert resp.status_code == 200
    repo.return_value.list_history.assert_called_once_with(
        is_paper=False, status="CLOSED", symbol="MSFT", limit=50, offset=5
    )


def test_list_trades_caps_limit(client: TestApp) -> None:
    with (
        patch("quant_execution.api.routes.trades.session_scope", _fake_scope),
        patch("quant_execution.api.routes.trades.TradesRepository") as repo,
    ):
        repo.return_value.list_history.return_value = []
        resp = client.get("/trades?limit=9999")

    assert resp.status_code == 200
    _, kwargs = repo.return_value.list_history.call_args
    assert kwargs["limit"] == 500


def test_list_trades_invalid_mode(client: TestApp) -> None:
    resp = client.get("/trades?mode=demo", expect_errors=True)
    assert resp.status_code == 400
    assert resp.json["code"] == "invalid_mode"


def test_list_trades_invalid_status(client: TestApp) -> None:
    resp = client.get("/trades?status=BOGUS", expect_errors=True)
    assert resp.status_code == 400
    assert resp.json["code"] == "invalid_status"


def test_list_trades_invalid_pagination(client: TestApp) -> None:
    resp = client.get("/trades?limit=abc", expect_errors=True)
    assert resp.status_code == 400
    assert resp.json["code"] == "invalid_pagination"


def test_get_trade_ok(client: TestApp) -> None:
    trade = _make_trade()
    with (
        patch("quant_execution.api.routes.trades.session_scope", _fake_scope),
        patch("quant_execution.api.routes.trades.TradesRepository") as repo,
    ):
        repo.return_value.get.return_value = trade
        resp = client.get(f"/trades/{trade.id}")

    assert resp.status_code == 200
    assert resp.json["trade"]["id"] == str(trade.id)
    assert resp.json["trade"]["symbol"] == "AAPL"


def test_get_trade_not_found(client: TestApp) -> None:
    with (
        patch("quant_execution.api.routes.trades.session_scope", _fake_scope),
        patch("quant_execution.api.routes.trades.TradesRepository") as repo,
    ):
        repo.return_value.get.return_value = None
        resp = client.get(f"/trades/{uuid.uuid4()}", expect_errors=True)

    assert resp.status_code == 404
    assert resp.json["code"] == "not_found"


def test_get_trade_invalid_id(client: TestApp) -> None:
    resp = client.get("/trades/not-a-uuid", expect_errors=True)
    assert resp.status_code == 400
    assert resp.json["code"] == "invalid_id"
