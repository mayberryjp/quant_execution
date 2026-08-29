"""HTTP client for the watchlist service ``quant_stickynote`` (SPEC.md §4.1).

Fetches active sticky notes via ``GET /sticky-notes/latest`` with offset/limit paging and returns
them as :class:`WatchlistEntry` objects. All network access is isolated here so the matching
engine can be unit-tested without I/O.
"""

from __future__ import annotations

import httpx

from quant_execution.domain.exceptions import WatchlistError
from quant_execution.domain.schemas import WatchlistEntry
from quant_execution.logging import get_logger

logger = get_logger(__name__)

_DEFAULT_PAGE_SIZE = 500
_MAX_PAGES = 10_000  # hard stop against a mis-paging server


class WatchlistClient:
    def __init__(
        self,
        base_url: str,
        *,
        page_size: int = _DEFAULT_PAGE_SIZE,
        timeout: float = 10.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._page_size = page_size
        self._timeout = timeout
        self._transport = transport

    def fetch_active(self) -> list[WatchlistEntry]:
        """Return all active watchlist entries, following pagination to exhaustion."""
        entries: list[WatchlistEntry] = []
        offset = 0
        with httpx.Client(timeout=self._timeout, transport=self._transport) as client:
            for _ in range(_MAX_PAGES):
                page = self._fetch_page(client, offset)
                if not page:
                    break
                entries.extend(page)
                if len(page) < self._page_size:
                    break
                offset += len(page)
        return entries

    def _fetch_page(self, client: httpx.Client, offset: int) -> list[WatchlistEntry]:
        url = f"{self._base_url}/sticky-notes/latest"
        params: dict[str, str | int] = {
            "limit": self._page_size,
            "offset": offset,
        }
        try:
            response = client.get(url, params=params)
            response.raise_for_status()
            payload = response.json()
        except httpx.HTTPError as exc:
            raise WatchlistError(f"watchlist fetch failed: {exc}") from exc
        except ValueError as exc:
            raise WatchlistError(f"watchlist returned invalid JSON: {exc}") from exc

        records = payload if isinstance(payload, list) else payload.get("items", [])
        return [WatchlistEntry.model_validate(record) for record in records]
