"""Executor worker entrypoint (SPEC.md §5.1 / §5.7).

Selects its mode (``paper``/``live``) from the ``--mode`` CLI argument. Both modes run concurrently
as separate supervisord programs sharing the same code path. It consumes ticks, matches them against
the in-memory watchlist to open positions, and checks each tick against the in-memory position book
to exit positions whose sell price is reached. Durable state is written by a non-blocking background
writer; live fills are confirmed by a reconciler; and a per-mode liquidator flattens every open
position at the configured market-close time.
"""

from __future__ import annotations

import argparse
import logging
import signal
import threading
from datetime import datetime
from decimal import Decimal
from types import FrameType
from zoneinfo import ZoneInfo

from quant_execution.clients.alpaca_client import AlpacaBroker
from quant_execution.clients.cash_client import CashClient
from quant_execution.clients.watchlist_client import WatchlistClient
from quant_execution.config import settings
from quant_execution.db import session_scope, wait_for_table
from quant_execution.domain.enums import ExecutionMode
from quant_execution.domain.matching import WatchlistRefresher, WatchlistStore
from quant_execution.domain.positions import PositionBook
from quant_execution.domain.schemas import Tick
from quant_execution.domain.services import (
    REHYDRATE_STATUSES,
    ExecutionService,
    MarketCloseLiquidator,
    PositionReconciler,
    parse_close_time,
    position_from_trade,
    shift_earlier,
)
from quant_execution.kafka.consumer import MessageMeta, TickConsumer, create_consumer
from quant_execution.logging import configure_logging, format_event, get_logger
from quant_execution.repository.async_writer import AsyncDbWriter
from quant_execution.repository.trades_repo import TradesRepository

VALID_MODES = tuple(m.value for m in ExecutionMode)

# Sampled tick heartbeat: log the first tick seen, then one in every _TICK_LOG_SAMPLE.
_TICK_LOG_SAMPLE = 1000
_stream_log = get_logger("executor.stream")
_tick_seen = 0

_MARKET_TZ = ZoneInfo(settings.market_timezone)


def is_trading_day(now: datetime | None = None) -> bool:
    """True on Mon-Fri in the market timezone; weekend ticks are dropped (no weekend runs)."""
    moment = now or datetime.now(_MARKET_TZ)
    return moment.weekday() < 5


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
    """Per-tick hot path: record price, open on match, exit on target. Raises on poison input."""
    if not is_trading_day():
        return
    if value is None:
        raise ValueError("empty message value")
    tick = Tick.parse(value)
    global _tick_seen
    _tick_seen += 1
    if _tick_seen == 1 or _tick_seen % _TICK_LOG_SAMPLE == 0:
        _stream_log.info(
            format_event("tick_stream", symbol=tick.symbol, price=tick.price, seen=_tick_seen)
        )
    service.record_price(tick.symbol, tick.price)
    for entry in store.match(tick.symbol, tick.price):
        service.execute(tick, entry, provenance=meta)
    service.check_exits(tick)


def rehydrate_positions(mode: ExecutionMode, service: ExecutionService, log: logging.Logger) -> None:
    """Rebuild the in-memory position book and dedup set from open trades (SPEC.md §5.7)."""
    try:
        with session_scope() as session:
            repo = TradesRepository(session)
            trades = repo.list_by_statuses(REHYDRATE_STATUSES, is_paper=mode.is_paper)
        keys = []
        for trade in trades:
            service.book.open(position_from_trade(trade))
            keys.append(trade.idempotency_key)
        service.seed(keys)
        log.info("rehydrated positions=%d", len(trades))
    except Exception:
        log.exception("position rehydration failed; starting with an empty book")


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
    open_raw = settings.market_open_paper if mode.is_paper else settings.market_open_live
    open_lead = (
        settings.market_open_lead_minutes_paper
        if mode.is_paper
        else settings.market_open_lead_minutes_live
    )
    open_time = shift_earlier(parse_close_time(open_raw), open_lead)
    refresher = WatchlistRefresher(
        store,
        watchlist_client.fetch_active,
        open_time,
        ZoneInfo(settings.market_timezone),
        check_seconds=float(settings.watchlist_refresh_seconds),
    )

    broker = AlpacaBroker(
        settings.alpaca_base_url, settings.alpaca_api_key, settings.alpaca_api_secret
    )
    cash: CashClient | None = None
    if not mode.is_paper:
        cash = CashClient(settings.cash_api_url, settings.cash_account_id)

    book = PositionBook()
    writer = AsyncDbWriter(
        batch_size=settings.db_writer_batch_size, queue_size=settings.db_writer_queue_size
    )
    service = ExecutionService(
        mode,
        broker,
        writer,
        book,
        notional_usd=settings.order_notional_usd,
        quantity=settings.order_quantity,
        cash=cash,
    )
    if not wait_for_table("trades"):
        log.warning("trades table not present after wait; continuing (writes may fail until migrated)")
    rehydrate_positions(mode, service, log)

    stop_event = threading.Event()
    _install_signal_handlers(stop_event, log)

    threading.Thread(target=writer.run_forever, args=(stop_event,), daemon=True).start()
    threading.Thread(target=refresher.run_forever, args=(stop_event,), daemon=True).start()
    log.info(
        "watchlist refresher armed trigger=%s (%dm before open=%s) tz=%s",
        open_time,
        open_lead,
        open_raw,
        settings.market_timezone,
    )

    if not mode.is_paper and cash is not None:
        reconciler = PositionReconciler(broker, cash, writer, book, on_close=service.forget)
        threading.Thread(
            target=reconciler.run_forever,
            args=(stop_event, float(settings.alpaca_poll_seconds)),
            daemon=True,
        ).start()
        log.info("reconciliation loop started interval=%ss", settings.alpaca_poll_seconds)

    close_raw = settings.market_close_paper if mode.is_paper else settings.market_close_live
    close_lead = (
        settings.market_close_lead_minutes_paper
        if mode.is_paper
        else settings.market_close_lead_minutes_live
    )
    close_time = shift_earlier(parse_close_time(close_raw), close_lead)
    liquidator = MarketCloseLiquidator(
        service,
        close_time,
        ZoneInfo(settings.market_timezone),
        check_seconds=float(settings.market_close_check_seconds),
    )
    threading.Thread(target=liquidator.run_forever, args=(stop_event,), daemon=True).start()
    log.info(
        "market close liquidator armed trigger=%s (%dm before close=%s) tz=%s",
        close_time,
        close_lead,
        close_raw,
        settings.market_timezone,
    )

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
