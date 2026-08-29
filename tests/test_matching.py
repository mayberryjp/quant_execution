"""Unit tests for the matching engine (SPEC.md §5.2). No I/O."""

from __future__ import annotations

import threading
from datetime import datetime
from datetime import time as clock_time
from decimal import Decimal
from zoneinfo import ZoneInfo

from quant_execution.domain.enums import PositionType
from quant_execution.domain.matching import (
    WatchlistRefresher,
    WatchlistStore,
    entry_matches,
)
from quant_execution.domain.schemas import WatchlistEntry


def _entry(symbol: str, price: str, position: PositionType, reason: str = "r") -> WatchlistEntry:
    return WatchlistEntry(
        symbol=symbol,
        buy_price=Decimal(price),
        sell_price=Decimal(price) * 2,
        position_type=position,
        trigger_reason=reason,
    )


def test_long_matches_at_or_below_buy_price() -> None:
    entry = _entry("AAPL", "100", PositionType.LONG)
    assert entry_matches(entry, Decimal(100), Decimal(0)) is True
    assert entry_matches(entry, Decimal("99.99"), Decimal(0)) is True
    assert entry_matches(entry, Decimal("100.01"), Decimal(0)) is False


def test_short_matches_at_or_above_buy_price() -> None:
    entry = _entry("AAPL", "100", PositionType.SHORT)
    assert entry_matches(entry, Decimal(100), Decimal(0)) is True
    assert entry_matches(entry, Decimal("100.01"), Decimal(0)) is True
    assert entry_matches(entry, Decimal("99.99"), Decimal(0)) is False


def test_tolerance_widens_the_band() -> None:
    long_entry = _entry("AAPL", "100", PositionType.LONG)
    assert entry_matches(long_entry, Decimal("100.5"), Decimal(1)) is True
    assert entry_matches(long_entry, Decimal("101.5"), Decimal(1)) is False

    short_entry = _entry("AAPL", "100", PositionType.SHORT)
    assert entry_matches(short_entry, Decimal("99.5"), Decimal(1)) is True
    assert entry_matches(short_entry, Decimal("98.5"), Decimal(1)) is False


def test_store_match_no_match_and_unknown_symbol() -> None:
    store = WatchlistStore()
    store.replace([_entry("AAPL", "100", PositionType.LONG)])
    assert len(store.match("AAPL", Decimal(99))) == 1
    assert store.match("AAPL", Decimal(101)) == []
    assert store.match("MSFT", Decimal(1)) == []


def test_store_multi_entry_symbol_arms_independently() -> None:
    store = WatchlistStore()
    store.replace(
        [
            _entry("AAPL", "100", PositionType.LONG, reason="a"),
            _entry("AAPL", "90", PositionType.LONG, reason="b"),
        ]
    )
    matched = store.match("AAPL", Decimal(95))
    assert {e.trigger_reason for e in matched} == {"a"}
    matched_low = store.match("AAPL", Decimal(85))
    assert {e.trigger_reason for e in matched_low} == {"a", "b"}


def test_refresh_swaps_snapshot_atomically() -> None:
    store = WatchlistStore()
    store.replace([_entry("AAPL", "100", PositionType.LONG)])
    assert store.symbols == {"AAPL"}

    refresher = WatchlistRefresher(
        store,
        fetcher=lambda: [_entry("MSFT", "50", PositionType.SHORT)],
        open_time=None,
        tz=ZoneInfo("UTC"),
        check_seconds=0.01,
    )
    count = refresher.refresh_once()
    assert count == 1
    assert store.symbols == {"MSFT"}


def test_run_forever_loads_at_startup_and_stops_on_event() -> None:
    store = WatchlistStore()
    calls = {"n": 0}

    def fetcher() -> list[WatchlistEntry]:
        calls["n"] += 1
        return []

    stop = threading.Event()
    refresher = WatchlistRefresher(
        store, fetcher=fetcher, open_time=None, tz=ZoneInfo("UTC"), check_seconds=0.01
    )

    def run() -> None:
        refresher.run_forever(stop)

    thread = threading.Thread(target=run)
    thread.start()
    stop.set()
    thread.join(timeout=2)
    assert not thread.is_alive()
    # A single startup load happens even with no scheduled open time.
    assert calls["n"] >= 1


def test_scheduled_refresh_fires_once_per_day_at_open() -> None:
    store = WatchlistStore()
    calls = {"n": 0}

    def fetcher() -> list[WatchlistEntry]:
        calls["n"] += 1
        return [_entry("AAPL", "100", PositionType.LONG)]

    tz = ZoneInfo("UTC")
    refresher = WatchlistRefresher(
        store, fetcher=fetcher, open_time=clock_time(9, 30), tz=tz, check_seconds=0.01
    )

    # Before open: no reload.
    assert refresher.maybe_refresh(datetime(2026, 1, 2, 9, 0, tzinfo=tz)) == 0
    assert calls["n"] == 0
    # At/after open: reload once.
    assert refresher.maybe_refresh(datetime(2026, 1, 2, 9, 30, tzinfo=tz)) == 1
    assert calls["n"] == 1
    # Same day, later: no second reload.
    assert refresher.maybe_refresh(datetime(2026, 1, 2, 15, 0, tzinfo=tz)) == 0
    assert calls["n"] == 1
    # Next day at open: reload again.
    assert refresher.maybe_refresh(datetime(2026, 1, 3, 9, 30, tzinfo=tz)) == 1
    assert calls["n"] == 2


def test_scheduled_refresh_disabled_when_open_time_none() -> None:
    store = WatchlistStore()
    refresher = WatchlistRefresher(
        store,
        fetcher=lambda: [_entry("AAPL", "100", PositionType.LONG)],
        open_time=None,
        tz=ZoneInfo("UTC"),
        check_seconds=0.01,
    )
    assert refresher.maybe_refresh(datetime(2026, 1, 2, 9, 30, tzinfo=ZoneInfo("UTC"))) == 0
