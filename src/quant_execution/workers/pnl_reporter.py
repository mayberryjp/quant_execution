"""Daily P&L reporter worker entrypoint.

A single process (both modes) that wakes on a fixed cadence and, at or after each mode's configured
market-close time, aggregates the day's closed trades into a ``daily_pnl`` row (paper and live are
recorded separately). It runs after the executors' market-close liquidation, so open positions have
already been flattened by the time the summary is taken.
"""

from __future__ import annotations

import logging
import signal
import threading
from types import FrameType
from zoneinfo import ZoneInfo

from quant_execution.config import settings
from quant_execution.db import wait_for_table
from quant_execution.domain.enums import ExecutionMode
from quant_execution.domain.pnl import DailyPnlReporter
from quant_execution.domain.services import parse_close_time
from quant_execution.logging import configure_logging, get_logger


def _install_signal_handlers(stop_event: threading.Event, log: logging.Logger) -> None:
    def _handle(signum: int, _frame: FrameType | None) -> None:
        log.info("received signal %d; shutting down", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)


def main() -> None:
    configure_logging(settings.log_level)
    log = get_logger("pnl_reporter")
    log.info("daily pnl reporter starting")

    tz = ZoneInfo(settings.market_timezone)
    schedule = [
        (ExecutionMode.PAPER, parse_close_time(settings.market_close_paper)),
        (ExecutionMode.LIVE, parse_close_time(settings.market_close_live)),
    ]
    reporter = DailyPnlReporter(
        schedule, tz, check_seconds=float(settings.pnl_report_check_seconds)
    )

    if not wait_for_table("daily_pnl"):
        log.warning("daily_pnl table not present after wait; continuing (writes may fail until migrated)")

    stop_event = threading.Event()
    _install_signal_handlers(stop_event, log)
    log.info(
        "daily pnl reporter armed paper_close=%s live_close=%s tz=%s check=%ss",
        settings.market_close_paper or "(disabled)",
        settings.market_close_live or "(disabled)",
        settings.market_timezone,
        settings.pnl_report_check_seconds,
    )
    reporter.run_forever(stop_event)
    log.info("daily pnl reporter stopped")


if __name__ == "__main__":
    main()
