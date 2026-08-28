"""Executor worker entrypoint (SPEC.md §5.1).

Selects its mode (``paper``/``live``) from the ``--mode`` CLI argument. Both modes run
concurrently as separate supervisord programs sharing the same code path. It consumes ticks,
matches them against the in-memory watchlist, and hands each match to the
:class:`ExecutionService` (which persists the trade and, for paper, records the assumed fill).
"""

from __future__ import annotations

import argparse
import logging
import signal
import threading
from decimal import Decimal
from types import FrameType

from quant_execution.clients.alpaca_client import AlpacaBroker
from quant_execution.clients.cash_client import CashClient
from quant_execution.clients.watchlist_client import WatchlistClient
from quant_execution.config import settings
from quant_execution.domain.enums import ExecutionMode
from quant_execution.domain.matching import WatchlistRefresher, WatchlistStore
from quant_execution.domain.schemas import Tick
from quant_execution.domain.services import ExecutionService, ReconciliationService
from quant_execution.kafka.consumer import MessageMeta, TickConsumer, create_consumer
from quant_execution.logging import configure_logging, get_logger

VALID_MODES = tuple(m.value for m in ExecutionMode)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="quant_execution.workers.executor")
    parser.add_argument("--mode", required=True, choices=VALID_MODES)
    return parser.parse_args(argv)


def process_message(
    value: bytes | None,
    meta: MessageMeta,
    store: WatchlistStore,
    service: ExecutionService,
) -> None:
    """Parse a tick, match it, and execute each match. Raises on malformed input (poison)."""
    if value is None:
        raise ValueError("empty message value")
    tick = Tick.parse(value)
    for entry in store.match(tick.symbol, tick.price):
        service.execute(tick, entry, provenance=meta)


def _install_signal_handlers(stop_event: threading.Event, log: logging.Logger) -> None:
    def _handle(signum: int, _frame: FrameType | None) -> None:
        log.info("received signal %d; shutting down", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)


def main() -> None:
    args = parse_args()
    configure_logging(settings.log_level)
    mode = ExecutionMode(args.mode)
    log = get_logger(f"executor.{mode}")
    log.info("executor starting in %s mode", mode)

    store = WatchlistStore(tolerance=Decimal(str(settings.price_match_tolerance)))
    watchlist_client = WatchlistClient(settings.watchlist_api_url)
    refresher = WatchlistRefresher(
        store, watchlist_client.fetch_active, settings.watchlist_refresh_seconds
    )

    broker = AlpacaBroker(
        settings.alpaca_base_url, settings.alpaca_api_key, settings.alpaca_api_secret
    )
    cash: CashClient | None = None
    if not mode.is_paper:
        cash = CashClient(settings.cash_api_url, settings.cash_account_id)
    service = ExecutionService(
        mode,
        broker,
        notional_usd=settings.order_notional_usd,
        quantity=settings.order_quantity,
        cash=cash,
    )

    stop_event = threading.Event()
    _install_signal_handlers(stop_event, log)

    refresher_thread = threading.Thread(
        target=refresher.run_forever, args=(stop_event,), daemon=True
    )
    refresher_thread.start()

    if not mode.is_paper and cash is not None:
        reconciler = ReconciliationService(broker, cash)
        reconciler_thread = threading.Thread(
            target=reconciler.run_forever,
            args=(stop_event, float(settings.alpaca_poll_seconds)),
            daemon=True,
        )
        reconciler_thread.start()
        log.info("reconciliation loop started interval=%ss", settings.alpaca_poll_seconds)

    topic = settings.kafka_topic_paper if mode.is_paper else settings.kafka_topic_live
    group_id = f"{settings.kafka_group_prefix}.{mode}"
    kafka_consumer = create_consumer(settings.kafka_bootstrap_servers, group_id)
    consumer = TickConsumer(kafka_consumer, topic)
    consumer.start()
    log.info("consuming topic=%s group=%s", topic, group_id)

    def handler(value: bytes | None, meta: MessageMeta) -> None:
        process_message(value, meta, store, service)

    consumer.run(handler, stop_event)
    log.info("executor stopped")


if __name__ == "__main__":
    main()

