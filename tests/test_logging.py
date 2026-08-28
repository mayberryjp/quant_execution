"""Tests for logging helpers (SLICE 6): logfmt event formatting."""

from __future__ import annotations

from quant_execution.logging import configure_logging, format_event


def test_configure_logging_runs() -> None:
    configure_logging("INFO")


def test_format_event_renders_fields_and_omits_none() -> None:
    line = format_event(
        "trade_executed",
        symbol="AAPL",
        mode="paper",
        broker_order_id=None,
        status="filled",
        duration_ms=1.23,
    )
    assert line == "trade_executed symbol=AAPL mode=paper status=filled duration_ms=1.23"


def test_format_event_quotes_values_with_spaces_or_equals() -> None:
    line = format_event("cash_error", detail="boom happened", key="a=b")
    assert 'detail="boom happened"' in line
    assert 'key="a=b"' in line
