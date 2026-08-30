"""Daily P&L routes: read-only access to the ``daily_pnl`` table this service owns."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from bottle import Bottle, request, response

from quant_execution.db import session_scope
from quant_execution.domain.enums import ExecutionMode
from quant_execution.repository.models import DailyPnl
from quant_execution.repository.pnl_repo import DailyPnlRepository

_DEFAULT_LIMIT = 100
_MAX_LIMIT = 500


def _to_jsonable(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def _serialize(row: DailyPnl) -> dict[str, Any]:
    return {
        column.name: _to_jsonable(getattr(row, column.name))
        for column in DailyPnl.__table__.columns
    }


def _error(status: int, code: str, message: str) -> dict[str, Any]:
    response.status = status
    return {"status": "error", "code": code, "error": message}


def register_pnl_routes(app: Bottle) -> None:
    @app.get("/pnl")
    def list_pnl() -> dict[str, Any]:
        params = request.query

        is_paper: bool | None = None
        mode_raw = params.get("mode") or None
        if mode_raw is not None:
            try:
                is_paper = ExecutionMode(mode_raw).is_paper
            except ValueError:
                return _error(400, "invalid_mode", f"unknown mode: {mode_raw!r} (use paper|live)")

        try:
            limit = int(params.get("limit") or _DEFAULT_LIMIT)
            offset = int(params.get("offset") or 0)
        except ValueError:
            return _error(400, "invalid_pagination", "limit and offset must be integers")
        if limit < 1 or offset < 0:
            return _error(400, "invalid_pagination", "limit must be >= 1 and offset >= 0")
        limit = min(limit, _MAX_LIMIT)

        with session_scope() as session:
            rows = DailyPnlRepository(session).list_history(
                is_paper=is_paper, limit=limit, offset=offset
            )
            items = [_serialize(row) for row in rows]

        return {"status": "ok", "count": len(items), "limit": limit, "offset": offset, "pnl": items}

    @app.get("/pnl/<trade_date>")
    def get_pnl(trade_date: str) -> dict[str, Any]:
        try:
            parsed = date.fromisoformat(trade_date)
        except ValueError:
            return _error(400, "invalid_date", f"not a valid date: {trade_date!r} (use YYYY-MM-DD)")

        with session_scope() as session:
            rows = DailyPnlRepository(session).list_for_date(parsed)
            if not rows:
                return _error(404, "not_found", f"no daily pnl for {trade_date}")
            items = [_serialize(row) for row in rows]

        return {"status": "ok", "trade_date": trade_date, "count": len(items), "pnl": items}
