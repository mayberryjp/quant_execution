"""Health and readiness routes."""

from __future__ import annotations

from typing import Any

from bottle import Bottle, response

from quant_execution.config import settings
from quant_execution.db import check_database

SERVICE_NAME = "quant-execution-api"


def register_health_routes(app: Bottle) -> None:
    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "service": SERVICE_NAME}

    @app.get("/ready")
    def ready() -> dict[str, Any]:
        ok, detail = check_database()
        if not ok:
            response.status = 503
            return {"status": "error", "code": "not_ready", "error": detail}
        missing = settings.missing_readiness_config()
        if missing:
            response.status = 503
            return {
                "status": "error",
                "code": "not_ready",
                "error": f"missing required config: {', '.join(missing)}",
            }
        return {"status": "ok"}

