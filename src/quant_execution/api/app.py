"""Bottle application factory."""

from __future__ import annotations

from typing import Any

from bottle import Bottle, response

from quant_execution.api.routes.health import register_health_routes
from quant_execution.api.routes.pnl import register_pnl_routes
from quant_execution.api.routes.trades import register_trade_routes

SERVICE_NAME = "quant-execution-api"


def create_app() -> Bottle:
    app = Bottle()
    app.title = SERVICE_NAME

    register_health_routes(app)
    register_trade_routes(app)
    register_pnl_routes(app)

    @app.hook("after_request")
    def _add_cors_headers() -> None:
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Origin, Content-Type, Accept, Authorization"

    @app.route("/<path:path>", method="OPTIONS")
    def _cors_preflight(path: str) -> str:
        return ""

    @app.error(404)
    def not_found(_err: Any) -> str:
        response.content_type = "application/json"
        return '{"status": "error", "code": "not_found", "error": "not found"}'

    return app


app = create_app()
