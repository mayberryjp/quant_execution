"""Unit tests for the watchlist client. Network is mocked via httpx MockTransport."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import httpx
import pytest

from quant_execution.clients.watchlist_client import WatchlistClient
from quant_execution.domain.exceptions import WatchlistError

Handler = Callable[[httpx.Request], httpx.Response]


def _client(handler: Handler, *, page_size: int = 2) -> WatchlistClient:
    return WatchlistClient(
        "http://watchlist.test",
        page_size=page_size,
        transport=httpx.MockTransport(handler),
    )


def test_fetch_active_follows_pagination() -> None:
    pages = {
        0: [
            {"symbol": "AAPL", "buy_price": "100", "position_type": "LONG"},
            {"symbol": "MSFT", "buy_price": "50", "position_type": "SHORT"},
        ],
        2: [{"symbol": "TSLA", "buy_price": "200", "position_type": "LONG"}],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params["offset"])
        return httpx.Response(200, json=pages.get(offset, []))

    entries = _client(handler, page_size=2).fetch_active()
    assert [e.symbol for e in entries] == ["AAPL", "MSFT", "TSLA"]
    assert entries[0].buy_price == Decimal(100)


def test_fetch_active_accepts_items_envelope() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params["offset"])
        if offset == 0:
            return httpx.Response(
                200,
                json={"items": [{"symbol": "AAPL", "buy_price": "10", "position_type": "LONG"}]},
            )
        return httpx.Response(200, json={"items": []})

    entries = _client(handler, page_size=1).fetch_active()
    assert len(entries) == 1


def test_fetch_active_raises_watchlist_error_on_http_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "boom"})

    with pytest.raises(WatchlistError):
        _client(handler).fetch_active()
