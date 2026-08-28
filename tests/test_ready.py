"""Tests for the /ready endpoint."""

from __future__ import annotations

from unittest.mock import patch

from webtest import TestApp

from quant_execution.config import Settings


def test_ready_ok_when_database_reachable(client: TestApp) -> None:
    with (
        patch("quant_execution.api.routes.health.check_database", return_value=(True, "ok")),
        patch.object(Settings, "missing_readiness_config", return_value=[]),
    ):
        resp = client.get("/ready")
    assert resp.status_code == 200
    assert resp.json["status"] == "ok"


def test_ready_503_when_database_unreachable(client: TestApp) -> None:
    with patch(
        "quant_execution.api.routes.health.check_database",
        return_value=(False, "database check failed: OperationalError"),
    ):
        resp = client.get("/ready", expect_errors=True)
    assert resp.status_code == 503
    assert resp.json["status"] == "error"
    assert resp.json["code"] == "not_ready"


def test_ready_503_when_live_config_missing(client: TestApp) -> None:
    with (
        patch("quant_execution.api.routes.health.check_database", return_value=(True, "ok")),
        patch.object(Settings, "missing_readiness_config", return_value=["EXEC_CASH_API_URL"]),
    ):
        resp = client.get("/ready", expect_errors=True)
    assert resp.status_code == 503
    assert resp.json["code"] == "not_ready"
    assert "EXEC_CASH_API_URL" in resp.json["error"]



