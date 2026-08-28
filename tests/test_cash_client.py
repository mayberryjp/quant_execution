"""Tests for the cash-service client (SLICE 5), using ``httpx.MockTransport`` (no network)."""

from __future__ import annotations

import uuid
from decimal import Decimal

import httpx
import pytest

from quant_execution.clients.cash_client import CashClient
from quant_execution.domain.exceptions import CashServiceError

_ACCOUNT = "acct-1"


def _client(transport: httpx.MockTransport, *, max_attempts: int = 1) -> CashClient:
    return CashClient(
        "https://cash.internal",
        _ACCOUNT,
        max_attempts=max_attempts,
        transport=transport,
    )


def test_get_available_balance_parses_decimal() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/accounts/{_ACCOUNT}/balance"
        return httpx.Response(200, json={"available_balance": "1234.56"})

    balance = _client(httpx.MockTransport(handle)).get_available_balance()
    assert balance == Decimal("1234.56")


def test_place_hold_posts_body_and_returns_id() -> None:
    hold_id = uuid.uuid4()
    seen: dict[str, object] = {}

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/holds"
        import json

        seen.update(json.loads(request.content))
        return httpx.Response(201, json={"id": str(hold_id)})

    hold = _client(httpx.MockTransport(handle)).place_hold(
        Decimal(100), reason="k", reference_id="k"
    )
    assert hold.id == hold_id
    assert seen["account_id"] == _ACCOUNT
    assert seen["amount"] == "100"
    assert seen["currency"] == "USD"
    assert seen["reason"] == "k"
    assert seen["reference_id"] == "k"


def test_capture_and_release_hold_hit_expected_paths() -> None:
    hold_id = uuid.uuid4()
    paths: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return httpx.Response(204)

    client = _client(httpx.MockTransport(handle))
    client.capture_hold(hold_id)
    client.release_hold(hold_id)
    assert paths == [f"/holds/{hold_id}/capture", f"/holds/{hold_id}/release"]


def test_client_error_raises_cash_service_error() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(422, json={"message": "invalid"})

    with pytest.raises(CashServiceError):
        _client(httpx.MockTransport(handle)).get_available_balance()


def test_server_error_retries_then_raises() -> None:
    calls = {"n": 0}

    def handle(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503, json={"message": "down"})

    with pytest.raises(CashServiceError):
        _client(httpx.MockTransport(handle), max_attempts=2).get_available_balance()
    assert calls["n"] == 2
