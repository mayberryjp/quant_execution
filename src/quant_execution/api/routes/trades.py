"""Trade history routes: read-only access to the ``trades`` table this service owns."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from bottle import Bottle, request, response

from quant_execution.db import session_scope
from quant_execution.domain.enums import ExecutionMode, TradeStatus
from quant_execution.repository.models import Trade
from quant_execution.repository.trades_repo import TradesRepository

_DEFAULT_LIMIT = 100
_MAX_LIMIT = 500

_VALID_STATUSES = frozenset(s.value for s in TradeStatus)


def _to_jsonable(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def _serialize_trade(trade: Trade) -> dict[str, Any]:
    return {
        column.name: _to_jsonable(getattr(trade, column.name))
        for column in Trade.__table__.columns
    }


def _error(status: int, code: str, message: str) -> dict[str, Any]:
    response.status = status
    return {"status": "error", "code": code, "error": message}


def register_trade_routes(app: Bottle) -> None:
    @app.get("/trades")
    def list_trades() -> dict[str, Any]:
        params = request.query

        is_paper: bool | None = None
        mode_raw = params.get("mode") or None
        if mode_raw is not None:
            try:
                is_paper = ExecutionMode(mode_raw).is_paper
            except ValueError:
                return _error(400, "invalid_mode", f"unknown mode: {mode_raw!r} (use paper|live)")

        status_filter = params.get("status") or None
        if status_filter is not None and status_filter not in _VALID_STATUSES:
            return _error(400, "invalid_status", f"unknown status: {status_filter!r}")

        symbol_filter = params.get("symbol") or None
        if symbol_filter is not None:
            symbol_filter = symbol_filter.upper()

        try:
            limit = int(params.get("limit") or _DEFAULT_LIMIT)
            offset = int(params.get("offset") or 0)
        except ValueError:
            return _error(400, "invalid_pagination", "limit and offset must be integers")
        if limit < 1 or offset < 0:
            return _error(400, "invalid_pagination", "limit must be >= 1 and offset >= 0")
        limit = min(limit, _MAX_LIMIT)

        with session_scope() as session:
            trades = TradesRepository(session).list_history(
                is_paper=is_paper,
                status=status_filter,
                symbol=symbol_filter,
                limit=limit,
                offset=offset,
            )
            items = [_serialize_trade(trade) for trade in trades]

        return {"status": "ok", "count": len(items), "limit": limit, "offset": offset, "trades": items}

    @app.get("/trades/<trade_id>")
    def get_trade(trade_id: str) -> dict[str, Any]:
        try:
            parsed = uuid.UUID(trade_id)
        except ValueError:
            return _error(400, "invalid_id", f"not a valid trade id: {trade_id!r}")

        with session_scope() as session:
            trade = TradesRepository(session).get(parsed)
            if trade is None:
                return _error(404, "not_found", f"no trade with id {trade_id}")
            item = _serialize_trade(trade)

        return {"status": "ok", "trade": item}
