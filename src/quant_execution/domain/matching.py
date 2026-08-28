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
from decimal import Decimal

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
    """Periodically reloads the store from a fetcher (SPEC.md §4.1 refresh loop).

    The fetcher is any zero-arg callable returning the current entries (typically
    ``WatchlistClient.fetch_active``), keeping this loop free of network concerns for testing.
    """

    def __init__(
        self,
        store: WatchlistStore,
        fetcher: Callable[[], list[WatchlistEntry]],
        interval_seconds: float,
    ) -> None:
        self._store = store
        self._fetcher = fetcher
        self._interval = interval_seconds

    def refresh_once(self) -> int:
        """Fetch and atomically swap the snapshot; return the entry count loaded."""
        entries = self._fetcher()
        self._store.replace(entries)
        logger.info("watchlist refreshed entries=%d symbols=%d", len(entries), len(self._store.symbols))
        return len(entries)

    def run_forever(self, stop_event: threading.Event) -> None:
        """Refresh immediately, then every ``interval_seconds`` until ``stop_event`` is set."""
        while not stop_event.is_set():
            try:
                self.refresh_once()
            except Exception:
                logger.exception("watchlist refresh failed; keeping previous snapshot")
            stop_event.wait(self._interval)
