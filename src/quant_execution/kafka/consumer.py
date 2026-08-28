"""Kafka consumer factory and consume loop (SPEC.md §5.1).

This service is purely a *consumer* of an existing Kafka cluster; it does not provision or manage
the broker. The loop is at-least-once: offsets are committed only after a message has been handled
(or deliberately skipped as poison), so a crash mid-processing redelivers the message and the
idempotency key guards against duplicate orders.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, cast

from quant_execution.logging import get_logger

logger = get_logger(__name__)

__all__ = ["MessageMeta", "TickConsumer", "create_consumer"]


@dataclass(frozen=True)
class MessageMeta:
    """Kafka provenance for a consumed message (SPEC.md §3.2)."""

    topic: str
    partition: int
    offset: int


class _ConsumerLike(Protocol):
    """The subset of ``confluent_kafka.Consumer`` this module depends on."""

    def subscribe(self, topics: list[str]) -> None: ...
    def poll(self, timeout: float) -> Any: ...
    def commit(self, *, message: Any, asynchronous: bool) -> Any: ...
    def close(self) -> None: ...


def create_consumer(
    bootstrap_servers: str,
    group_id: str,
    *,
    auto_offset_reset: str = "latest",
) -> _ConsumerLike:
    """Build a manual-commit ``confluent_kafka.Consumer`` (imported lazily)."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": auto_offset_reset,
        }
    )
    return cast("_ConsumerLike", consumer)


# handler(value_bytes, meta) -> None. Raising signals a poison message: logged and skipped.
Handler = Callable[[bytes | None, MessageMeta], None]


class TickConsumer:
    """Wraps a Kafka consumer with a graceful, at-least-once consume loop."""

    def __init__(
        self,
        consumer: _ConsumerLike,
        topic: str,
        *,
        poll_timeout: float = 1.0,
    ) -> None:
        self._consumer = consumer
        self._topic = topic
        self._poll_timeout = poll_timeout

    def start(self) -> None:
        self._consumer.subscribe([self._topic])

    def run(self, handler: Handler, stop_event: threading.Event) -> None:
        """Poll until ``stop_event`` is set; commit after each handled message."""
        try:
            while not stop_event.is_set():
                message = self._consumer.poll(self._poll_timeout)
                if message is None:
                    continue
                if message.error() is not None:
                    logger.error("kafka poll error: %s", message.error())
                    continue
                meta = MessageMeta(
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                )
                try:
                    handler(message.value(), meta)
                except Exception:  # poison message: log, skip, and commit past it
                    logger.exception(
                        "skipping unprocessable message topic=%s partition=%s offset=%s",
                        meta.topic,
                        meta.partition,
                        meta.offset,
                    )
                self._consumer.commit(message=message, asynchronous=False)
        finally:
            self._consumer.close()
