"""Bottle application factory."""

from __future__ import annotations

from typing import Any

from bottle import Bottle, response

from quant_execution.api.routes.health import register_health_routes
from quant_execution.api.routes.trades import register_trade_routes

SERVICE_NAME = "quant-execution-api"


def create_app() -> Bottle:
    app = Bottle()
    app.title = SERVICE_NAME

    register_health_routes(app)
    register_trade_routes(app)

    @app.error(404)
    def not_found(_err: Any) -> str:
        response.content_type = "application/json"
        return '{"status": "error", "code": "not_found", "error": "not found"}'

    return app


app = create_app()
