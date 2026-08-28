"""Unit tests for the non-blocking async DB writer (SPEC.md §5.7). No real database."""

from __future__ import annotations

import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager

from quant_execution.repository.async_writer import AsyncDbWriter
from quant_execution.repository.models import Trade


class FakeRepo:
    def __init__(self) -> None:
        self.inserted: list[Trade] = []
        self.updates: list[tuple[uuid.UUID, dict[str, object]]] = []

    def insert(self, trade: Trade) -> Trade:
        self.inserted.append(trade)
        return trade

    def apply_update(self, trade_id: uuid.UUID, /, **fields: object) -> None:
        self.updates.append((trade_id, fields))


def _uow(repo: FakeRepo):
    @contextmanager
    def factory() -> Iterator[FakeRepo]:
        yield repo

    return factory


def test_run_forever_drains_insert_then_update_in_order() -> None:
    repo = FakeRepo()
    writer = AsyncDbWriter(unit_of_work=_uow(repo), batch_size=10, poll_seconds=0.01)  # type: ignore[arg-type]
    trade_id = uuid.uuid4()
    trade = Trade(id=trade_id, status="NEW")
    writer.insert(trade)
    writer.update(trade_id, status="FILLED")

    stop = threading.Event()
    thread = threading.Thread(target=writer.run_forever, args=(stop,))
    thread.start()
    while len(repo.inserted) < 1 or len(repo.updates) < 1:
        stop.wait(0.01)
        if not thread.is_alive():
            break
    stop.set()
    thread.join(timeout=2)

    assert repo.inserted == [trade]
    assert repo.updates == [(trade_id, {"status": "FILLED"})]


def test_full_queue_drops_write_without_blocking() -> None:
    repo = FakeRepo()
    writer = AsyncDbWriter(unit_of_work=_uow(repo), queue_size=1)  # type: ignore[arg-type]
    first = Trade(id=uuid.uuid4(), status="NEW")
    second = Trade(id=uuid.uuid4(), status="NEW")
    writer.insert(first)  # fills the queue
    writer.insert(second)  # dropped, must not raise or block

    # Only the first task should remain queued; draining once applies just it.
    batch = writer._collect_batch(block=False)
    writer._flush(batch)

    assert repo.inserted == [first]
