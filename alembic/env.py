"""Alembic environment.

Race-safety and shared-database rules (see SPEC.md §6):
- A service-specific version table (``alembic_version_quant_execution``) keeps this service's
  migration history independent inside the shared Postgres database.
- ``include_object`` restricts migrations to this service's own tables.
- Online migrations acquire a transaction-scoped Postgres advisory lock so that concurrent
  migrators (multiple replicas/containers) serialize; the loser sees head and no-ops.
"""

from __future__ import annotations

import os

from sqlalchemy import engine_from_config, pool, text
from sqlalchemy.engine import Connection

# Import model modules so their tables are registered on Base.metadata.
import quant_execution.repository.models  # noqa: F401  (side-effect: register tables)
from alembic import context
from quant_execution.config import settings
from quant_execution.db import Base

# Unique per service so multiple projects can safely share one database.
VERSION_TABLE = "alembic_version_quant_execution"
# This service owns a dedicated schema in the shared database; its version table and all
# of its tables live there so nothing collides with other projects in ``public``.
VERSION_TABLE_SCHEMA = settings.db_schema

# Where this service's tables lived before it adopted a dedicated schema: the shared default
# ``public`` schema. Legacy databases are relocated out of it on the next migration.
LEGACY_SCHEMA = "public"

# Deterministic advisory-lock key for this service's migrations.
MIGRATION_LOCK_KEY = 528374091

target_metadata = Base.metadata


def include_object(
    obj: object, name: str | None, type_: str, reflected: bool, compare_to: object
) -> bool:
    if type_ == "table":
        return getattr(obj, "schema", None) == VERSION_TABLE_SCHEMA
    return True


def relocate_legacy_tables(connection: Connection) -> None:
    """Move this service's tables out of the legacy ``public`` schema into its dedicated one.

    Databases created before this service adopted a dedicated schema keep their tables — and this
    service's private version table — in ``public``. ``ALTER TABLE ... SET SCHEMA`` relocates each
    one in place, preserving every row. This must run before Alembic reads its version table so a
    legacy database is recognized as already-migrated instead of having empty tables recreated in
    the new schema (which would orphan the existing data).

    Idempotent: a table is moved only when it still exists in ``public`` and is not already present
    in the target schema, so fresh and already-migrated databases are no-ops. Only tables this
    service owns are touched; other services' tables in the shared ``public`` schema are left alone.
    """
    if VERSION_TABLE_SCHEMA == LEGACY_SCHEMA:
        return
    owned = [table.name for table in target_metadata.tables.values()]
    owned.append(VERSION_TABLE)
    for name in owned:
        needs_move = connection.execute(
            text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = :legacy AND table_name = :name "
                "AND table_type = 'BASE TABLE' "
                "AND NOT EXISTS ("
                "  SELECT 1 FROM information_schema.tables "
                "  WHERE table_schema = :target AND table_name = :name"
                ")"
            ),
            {"legacy": LEGACY_SCHEMA, "target": VERSION_TABLE_SCHEMA, "name": name},
        ).first()
        if needs_move:
            connection.execute(
                text(
                    f'ALTER TABLE "{LEGACY_SCHEMA}"."{name}" '
                    f'SET SCHEMA "{VERSION_TABLE_SCHEMA}"'
                )
            )


def run_migrations_offline() -> None:
    context.configure(
        url=os.environ["DATABASE_URL"],
        target_metadata=target_metadata,
        version_table=VERSION_TABLE,
        version_table_schema=VERSION_TABLE_SCHEMA,
        include_object=include_object,
        include_schemas=True,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    config = context.config
    config.set_main_option("sqlalchemy.url", os.environ["DATABASE_URL"])
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table=VERSION_TABLE,
            version_table_schema=VERSION_TABLE_SCHEMA,
            include_object=include_object,
            include_schemas=True,
        )
        # Nothing must run on the connection before begin_transaction, otherwise SQLAlchemy
        # autobegins a transaction that Alembic treats as externally managed and never
        # commits. Alembic owns (and commits) this transaction.
        with context.begin_transaction():
            # Transaction-scoped lock serializes concurrent migrators; auto-released on commit.
            connection.execute(
                text("SELECT pg_advisory_xact_lock(:k)"), {"k": MIGRATION_LOCK_KEY}
            )
            # Create this service's schema before Alembic creates its version table inside
            # it, and route the unqualified migration DDL there.
            connection.execute(
                text(f'CREATE SCHEMA IF NOT EXISTS "{VERSION_TABLE_SCHEMA}"')
            )
            # Relocate any pre-existing tables (and this service's version table) out of the
            # legacy ``public`` schema before Alembic reads its version table, so existing data is
            # preserved rather than orphaned by fresh table creation in the new schema.
            relocate_legacy_tables(connection)
            connection.execute(
                text(f'SET search_path TO "{VERSION_TABLE_SCHEMA}", public')
            )
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
