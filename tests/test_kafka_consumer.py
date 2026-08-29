"""Tests for the Kafka consume loop (no real broker) and tick parsing (SLICE 3)."""

from __future__ import annotations

import threading
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from quant_execution.domain.schemas import Tick
from quant_execution.kafka.consumer import MessageMeta, TickConsumer


class FakeMessage:
    def __init__(
        self,
        value: bytes | None,
        *,
        error: object | None = None,
        topic: str = "ticks.paper",
        partition: int = 0,
        offset: int = 0,
    ) -> None:
        self._value = value
        self._error = error
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def value(self) -> bytes | None:
        return self._value

    def error(self) -> object | None:
        return self._error

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class FakeConsumer:
    def __init__(self, messages: list[FakeMessage | None]) -> None:
        self._messages = list(messages)
        self.subscribed: list[str] = []
        self.committed: list[FakeMessage] = []
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = list(topics)

    def poll(self, timeout: float) -> FakeMessage | None:
        if self._messages:
            return self._messages.pop(0)
        return None

    def commit(self, *, message: FakeMessage, asynchronous: bool) -> None:
        self.committed.append(message)

    def close(self) -> None:
        self.closed = True


def test_tick_parse_valid() -> None:
    tick = Tick.parse(b'{"symbol": "AAPL", "price": "99.5", "ts": "2024-01-02T00:00:00Z"}')
    assert tick.symbol == "AAPL"
    assert tick.price == Decimal("99.5")


def test_tick_parse_streamingchart_bar() -> None:
    raw = (
        b'{"schema_version": 1, "session_id": 123, "ticker": "MSFT", "sequence": 0,'
        b' "interval": "1m", "bar_time": "2026-08-28T13:30:00Z", "open": 100.1,'
        b' "high": 100.5, "low": 99.9, "close": 100.3, "volume": 12345,'
        b' "emitted_at": "2026-08-29T02:00:00Z", "is_first": true, "is_last": false}'
    )
    tick = Tick.parse(raw)
    assert tick.symbol == "MSFT"
    assert tick.price == Decimal("100.3")
    assert tick.ts == datetime(2026, 8, 28, 13, 30, tzinfo=UTC)


def test_tick_parse_malformed_raises() -> None:
    with pytest.raises(ValueError):
        Tick.parse(b"not json")


def test_consumer_loop_commits_and_skips_poison() -> None:
    good = FakeMessage(b'{"symbol": "AAPL", "price": "99"}', offset=1)
    poison = FakeMessage(b"garbage", offset=2)
    errored = FakeMessage(None, error=object(), offset=3)
    fake = FakeConsumer([good, poison, errored])

    stop_event = threading.Event()
    handled: list[MessageMeta] = []

    def handler(value: bytes | None, meta: MessageMeta) -> None:
        if value == b"garbage":
            raise ValueError("poison")
        handled.append(meta)

    def run() -> None:
        consumer = TickConsumer(fake, "ticks.paper", poll_timeout=0.01)
        consumer.start()
        consumer.run(handler, stop_event)

    thread = threading.Thread(target=run)
    thread.start()
    # Let it drain the three messages, then stop.
    while len(fake.committed) < 2 and thread.is_alive():
        stop_event.wait(0.01)
    stop_event.set()
    thread.join(timeout=2)

    assert fake.subscribed == ["ticks.paper"]
    # good + poison are consumed and committed; errored message is not committed.
    assert [m.offset() for m in fake.committed] == [1, 2]
    assert [m.offset for m in handled] == [1]
    assert fake.closed is True
