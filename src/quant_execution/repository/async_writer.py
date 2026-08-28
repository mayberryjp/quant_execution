"""Non-blocking database writer (SPEC.md §5.7).

The streaming path must never block on the database. State changes are enqueued here and applied by
a single background thread that batches many writes into one transaction. The in-memory
:class:`~quant_execution.domain.positions.PositionBook` is the source of truth; the database is an
asynchronous, eventually-consistent projection, so a full queue drops the write (and logs) rather
than stalling the tick loop.

Ordering is preserved (a single FIFO queue drained by one worker), so an insert always lands before
the updates that follow it for the same trade id.
"""

from __future__ import annotations

import queue
import threading
import uuid
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass

from quant_execution.db import session_scope
from quant_execution.logging import get_logger
from quant_execution.repository.models import Trade
from quant_execution.repository.trades_repo import TradesRepository

__all__ = ["AsyncDbWriter", "TradeWriter", "WriterUnitOfWork"]

logger = get_logger(__name__)

WriterUnitOfWork = Callable[[], AbstractContextManager[TradesRepository]]


@contextmanager
def _default_writer_unit_of_work() -> Iterator[TradesRepository]:
    with session_scope() as session:
        yield TradesRepository(session)


@dataclass(frozen=True)
class _InsertTask:
    trade: Trade


@dataclass(frozen=True)
class _UpdateTask:
    trade_id: uuid.UUID
    fields: dict[str, object]


_Task = _InsertTask | _UpdateTask


class TradeWriter:
    """Interface used by services so they can be unit-tested with a synchronous fake."""

    def insert(self, trade: Trade) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def update(self, trade_id: uuid.UUID, /, **fields: object) -> None:  # pragma: no cover
        raise NotImplementedError


class AsyncDbWriter(TradeWriter):
    """Queue-backed writer drained by a background thread in batched transactions."""

    def __init__(
        self,
        *,
        unit_of_work: WriterUnitOfWork = _default_writer_unit_of_work,
        batch_size: int = 200,
        queue_size: int = 100_000,
        poll_seconds: float = 0.5,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._batch_size = batch_size
        self._poll_seconds = poll_seconds
        self._queue: queue.Queue[_Task] = queue.Queue(maxsize=queue_size)

    def insert(self, trade: Trade) -> None:
        self._enqueue(_InsertTask(trade))

    def update(self, trade_id: uuid.UUID, /, **fields: object) -> None:
        self._enqueue(_UpdateTask(trade_id, fields))

    def _enqueue(self, task: _Task) -> None:
        try:
            self._queue.put_nowait(task)
        except queue.Full:
            logger.error("db writer queue full; dropping write task=%s", type(task).__name__)

    def run_forever(self, stop_event: threading.Event) -> None:
        """Drain and apply batches until stopped, then flush whatever remains."""
        while not stop_event.is_set():
            batch = self._collect_batch()
            if batch:
                self._flush(batch)
        remaining = self._collect_batch(block=False)
        if remaining:
            self._flush(remaining)

    def _collect_batch(self, *, block: bool = True) -> list[_Task]:
        batch: list[_Task] = []
        try:
            first = self._queue.get(timeout=self._poll_seconds) if block else self._queue.get_nowait()
        except queue.Empty:
            return batch
        batch.append(first)
        while len(batch) < self._batch_size:
            try:
                batch.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return batch

    def _flush(self, batch: list[_Task]) -> None:
        try:
            with self._unit_of_work() as repo:
                for task in batch:
                    if isinstance(task, _InsertTask):
                        repo.insert(task.trade)
                    else:
                        repo.apply_update(task.trade_id, **task.fields)
        except Exception:
            logger.exception("db writer batch failed size=%d; entries dropped", len(batch))
