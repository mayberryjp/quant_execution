"""Tests for the /health endpoint."""

from __future__ import annotations

from webtest import TestApp


def test_health_ok(client: TestApp) -> None:
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json["status"] == "ok"
    assert resp.json["service"] == "quant-execution-api"
