"""In-memory watchlist store and match rules (SPEC.md §5.2).

The store holds an immutable snapshot keyed by symbol for O(1) matching per tick. Refresh builds
a new snapshot and atomically swaps the reference, so a concurrent match never sees a partial
update.

Match rules:
- ``LONG`` entry arms a BUY when ``price <= buy_price``.
- ``SHORT`` entry arms a SELL when ``price >= buy_price``.
- An optional absolute ``tolerance`` widens the trigger band on the matching side.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from collections.abc import Callable
from datetime import date, datetime
from datetime import time as clock_time
from decimal import Decimal
from zoneinfo import ZoneInfo

from quant_execution.domain.enums import PositionType
from quant_execution.domain.schemas import WatchlistEntry
from quant_execution.logging import get_logger

__all__ = ["WatchlistRefresher", "WatchlistStore", "entry_matches"]

logger = get_logger(__name__)


def entry_matches(entry: WatchlistEntry, price: Decimal, tolerance: Decimal) -> bool:
    if entry.position_type is PositionType.LONG:
        return price <= entry.buy_price + tolerance
    return price >= entry.buy_price - tolerance


class WatchlistStore:
    """Thread-safe-by-swap snapshot of armed watchlist entries."""

    def __init__(self, tolerance: Decimal = Decimal(0)) -> None:
        self._tolerance = tolerance
        self._by_symbol: dict[str, list[WatchlistEntry]] = {}

    def replace(self, entries: list[WatchlistEntry]) -> None:
        """Atomically replace the snapshot with a new set of entries."""
        snapshot: dict[str, list[WatchlistEntry]] = defaultdict(list)
        for entry in entries:
            snapshot[entry.symbol].append(entry)
        self._by_symbol = dict(snapshot)

    def __len__(self) -> int:
        return sum(len(v) for v in self._by_symbol.values())

    @property
    def symbols(self) -> set[str]:
        return set(self._by_symbol)

    def match(self, symbol: str, price: Decimal) -> list[WatchlistEntry]:
        """Return every armed entry for ``symbol`` triggered by ``price`` (O(1) lookup)."""
        candidates = self._by_symbol.get(symbol)
        if not candidates:
            return []
        return [e for e in candidates if entry_matches(e, price, self._tolerance)]


class WatchlistRefresher:
    """Reloads the store from a fetcher once per session at market open (SPEC.md §4.1).

    Execution only needs the sticky note right before a trading session begins, so instead of
    polling continuously it reloads once per day at/after ``open_time`` (in ``tz``). One load is
    also performed at startup so a restart mid-session is never left with an empty watchlist.
    ``check_seconds`` is only the clock-check cadence; the network fetch happens at startup and
    once per day at open. An ``open_time`` of ``None`` disables the scheduled reload (startup
    load only). The fetcher is any zero-arg callable returning the current entries (typically
    ``WatchlistClient.fetch_active``), keeping this loop free of network concerns for testing.
    """

    def __init__(
        self,
        store: WatchlistStore,
        fetcher: Callable[[], list[WatchlistEntry]],
        open_time: clock_time | None,
        tz: ZoneInfo,
        *,
        check_seconds: float = 30.0,
    ) -> None:
        self._store = store
        self._fetcher = fetcher
        self._open_time = open_time
        self._tz = tz
        self._check_seconds = check_seconds
        self._last_run: date | None = None

    def refresh_once(self) -> int:
        """Fetch and atomically swap the snapshot; return the entry count loaded."""
        entries = self._fetcher()
        self._store.replace(entries)
        logger.info("watchlist refreshed entries=%d symbols=%d", len(entries), len(self._store.symbols))
        return len(entries)

    def maybe_refresh(self, now: datetime | None = None) -> int:
        """Reload if we are at/after the open time and have not already done so today."""
        if self._open_time is None:
            return 0
        moment = now or datetime.now(self._tz)
        today = moment.date()
        if moment.time() >= self._open_time and self._last_run != today:
            self._last_run = today
            return self.refresh_once()
        return 0

    def run_forever(self, stop_event: threading.Event) -> None:
        """Load once at startup, then reload once per day at open until ``stop_event`` is set."""
        try:
            self.refresh_once()
        except Exception:
            logger.exception("watchlist refresh failed; keeping previous snapshot")
        # If we started at/after today's open, the startup load covers this session; don't reload
        # again until tomorrow's open. Starting before open leaves ``_last_run`` unset so the open
        # reload still fires.
        if self._open_time is not None:
            now = datetime.now(self._tz)
            if now.time() >= self._open_time:
                self._last_run = now.date()
        while not stop_event.is_set():
            try:
                self.maybe_refresh()
            except Exception:
                logger.exception("watchlist refresh failed; keeping previous snapshot")
            stop_event.wait(self._check_seconds)
