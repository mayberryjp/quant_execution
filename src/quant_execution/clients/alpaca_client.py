"""HTTP client for the Alpaca broker (SPEC.md §4.3).

Wraps the two calls the service needs: ``submit_order`` (entry) and ``get_order`` (reconciliation).
The trade ``idempotency_key`` is passed as Alpaca's ``client_order_id`` so broker-side dedup aligns
with our database dedup. All network access is isolated here and bounded with a timeout and a small
number of retries on transient failures.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from decimal import Decimal

import httpx
from pydantic import BaseModel

from quant_execution.domain.enums import OrderSide
from quant_execution.domain.exceptions import BrokerError
from quant_execution.logging import get_logger

logger = get_logger(__name__)

_BROKER_NAME = "alpaca"
_MAX_ATTEMPTS = 3
_RETRY_BACKOFF_SECONDS = 0.5


class AlpacaOrder(BaseModel):
    """The subset of an Alpaca order response the service consumes."""

    id: str
    status: str
    filled_qty: Decimal | None = None
    filled_avg_price: Decimal | None = None


class AlpacaBroker:
    name = _BROKER_NAME

    def __init__(
        self,
        base_url: str,
        api_key: str,
        api_secret: str,
        *,
        timeout: float = 10.0,
        max_attempts: int = _MAX_ATTEMPTS,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_attempts = max_attempts
        self._transport = transport
        self._headers = {
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": api_secret,
        }

    def submit_order(
        self,
        *,
        symbol: str,
        qty: Decimal,
        side: OrderSide,
        client_order_id: str,
        order_type: str = "market",
        time_in_force: str = "day",
    ) -> AlpacaOrder:
        """Submit an order and return the created order (SPEC.md §4.3)."""
        body = {
            "symbol": symbol,
            "qty": str(qty),
            "side": side.value.lower(),
            "type": order_type,
            "time_in_force": time_in_force,
            "client_order_id": client_order_id,
        }
        payload = self._request("POST", "/v2/orders", json_body=body)
        return AlpacaOrder.model_validate(payload)

    def get_order(self, broker_order_id: str) -> AlpacaOrder:
        """Fetch an order by broker id for reconciliation (SPEC.md §4.3)."""
        payload = self._request("GET", f"/v2/orders/{broker_order_id}")
        return AlpacaOrder.model_validate(payload)

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Mapping[str, object] | None = None,
    ) -> object:
        url = f"{self._base_url}{path}"
        last_exc: Exception | None = None
        with httpx.Client(
            timeout=self._timeout, transport=self._transport, headers=self._headers
        ) as client:
            for attempt in range(1, self._max_attempts + 1):
                try:
                    response = client.request(method, url, json=json_body)
                    response.raise_for_status()
                    return response.json()
                except httpx.HTTPStatusError as exc:
                    # 4xx are deterministic client errors; do not retry.
                    if exc.response.status_code < 500:
                        raise BrokerError(
                            f"alpaca {method} {path} rejected: {exc.response.status_code}"
                        ) from exc
                    last_exc = exc
                except (httpx.TransportError, ValueError) as exc:
                    last_exc = exc
                if attempt < self._max_attempts:
                    time.sleep(_RETRY_BACKOFF_SECONDS * attempt)
        raise BrokerError(
            f"alpaca {method} {path} failed after {self._max_attempts} attempts: {last_exc}"
        )
