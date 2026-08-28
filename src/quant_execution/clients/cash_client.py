"""HTTP client for the cash service ``quant_cash`` (SPEC.md §4.2), used by the live path only.

Reserves funds with a hold before an order is submitted to the broker, then captures the hold on a
confirmed fill or releases it on reject/cancel. All network access is isolated here and bounded with
a timeout and a small number of retries on transient failures.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Mapping
from decimal import Decimal

import httpx
from pydantic import BaseModel

from quant_execution.domain.exceptions import CashServiceError
from quant_execution.logging import get_logger

logger = get_logger(__name__)

_DEFAULT_CURRENCY = "USD"
_MAX_ATTEMPTS = 3
_RETRY_BACKOFF_SECONDS = 0.5


class CashHold(BaseModel):
    """The subset of a cash-hold response the service consumes."""

    id: uuid.UUID


class CashClient:
    def __init__(
        self,
        base_url: str,
        account_id: str,
        *,
        currency: str = _DEFAULT_CURRENCY,
        timeout: float = 10.0,
        max_attempts: int = _MAX_ATTEMPTS,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._account_id = account_id
        self._currency = currency
        self._timeout = timeout
        self._max_attempts = max_attempts
        self._transport = transport

    def get_available_balance(self) -> Decimal:
        """Return the account's available balance (SPEC.md §4.2)."""
        payload = self._request("GET", f"/accounts/{self._account_id}/balance")
        if not isinstance(payload, Mapping) or "available_balance" not in payload:
            raise CashServiceError("cash balance response missing available_balance")
        return Decimal(str(payload["available_balance"]))

    def place_hold(self, amount: Decimal, *, reason: str, reference_id: str) -> CashHold:
        """Reserve ``amount`` for an order before broker submission (SPEC.md §4.2)."""
        body = {
            "account_id": self._account_id,
            "amount": str(amount),
            "currency": self._currency,
            "reason": reason,
            "reference_id": reference_id,
        }
        payload = self._request("POST", "/holds", json_body=body)
        return CashHold.model_validate(payload)

    def capture_hold(self, hold_id: uuid.UUID) -> None:
        """Capture a hold on a confirmed fill (SPEC.md §4.2)."""
        self._request("POST", f"/holds/{hold_id}/capture")

    def release_hold(self, hold_id: uuid.UUID) -> None:
        """Release a hold on reject/cancel (SPEC.md §4.2)."""
        self._request("POST", f"/holds/{hold_id}/release")

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Mapping[str, object] | None = None,
    ) -> object:
        url = f"{self._base_url}{path}"
        last_exc: Exception | None = None
        with httpx.Client(timeout=self._timeout, transport=self._transport) as client:
            for attempt in range(1, self._max_attempts + 1):
                try:
                    response = client.request(method, url, json=json_body)
                    response.raise_for_status()
                    return response.json() if response.content else None
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code < 500:
                        raise CashServiceError(
                            f"cash {method} {path} rejected: {exc.response.status_code}"
                        ) from exc
                    last_exc = exc
                except (httpx.TransportError, ValueError) as exc:
                    last_exc = exc
                if attempt < self._max_attempts:
                    time.sleep(_RETRY_BACKOFF_SECONDS * attempt)
        raise CashServiceError(
            f"cash {method} {path} failed after {self._max_attempts} attempts: {last_exc}"
        )
