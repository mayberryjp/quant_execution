"""Database engine, session helpers, and the declarative ``Base``.

The Postgres database is shared across services. This service owns only its own tables and
never reads, writes, or migrates another service's tables.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import MetaData, create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from quant_execution.config import settings


class Base(DeclarativeBase):
    """Declarative base for all ORM models owned by this service."""

    # All tables live in this service's dedicated schema in the shared database.
    metadata = MetaData(schema=settings.db_schema)


_engine: Engine | None = None
_Session: sessionmaker[Session] | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(
            settings.database_url,
            pool_pre_ping=True,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            connect_args={"options": f"-c search_path={settings.db_schema},public"},
        )
    return _engine


def get_sessionmaker() -> sessionmaker[Session]:
    global _Session
    if _Session is None:
        _Session = sessionmaker(bind=get_engine(), expire_on_commit=False)
    return _Session


@contextmanager
def session_scope() -> Iterator[Session]:
    """Transactional scope: commit on success, rollback on error."""
    session = get_sessionmaker()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def check_database() -> tuple[bool, str]:
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, "ok"
    except SQLAlchemyError as exc:
        return False, f"database check failed: {type(exc).__name__}"


def wait_for_table(table: str, *, timeout: float = 60.0, interval: float = 1.0) -> bool:
    """Block until ``table`` exists so startup never races the migrate step (returns readiness)."""
    engine = get_engine()
    deadline = time.monotonic() + timeout
    while True:
        try:
            if inspect(engine).has_table(table, schema=settings.db_schema):
                return True
        except SQLAlchemyError:
            pass
        if time.monotonic() >= deadline:
            return False
        time.sleep(interval)
