"""Alembic environment.

Race-safety and shared-database rules (see SPEC.md §6):
- A service-specific version table (``alembic_version_quant_execution``) keeps this service's
  migration history independent inside the shared Postgres database.
- ``include_object`` restricts migrations to this service's own tables.
- Online migrations acquire a Postgres advisory lock so that concurrent migrators (multiple
  replicas/containers) serialize; the loser sees head and no-ops.
"""

from __future__ import annotations

import os

from sqlalchemy import engine_from_config, pool, text

# Import model modules so their tables are registered on Base.metadata.
import quant_execution.repository.models  # noqa: F401  (side-effect: register tables)
from alembic import context
from quant_execution.db import Base

# Unique per service so multiple projects can safely share one database.
VERSION_TABLE = "alembic_version_quant_execution"
VERSION_TABLE_SCHEMA = None

# Deterministic advisory-lock key for this service's migrations.
MIGRATION_LOCK_KEY = 528374091

target_metadata = Base.metadata


def include_object(
    obj: object, name: str | None, type_: str, reflected: bool, compare_to: object
) -> bool:
    if type_ == "table":
        return name in target_metadata.tables
    return True


def run_migrations_offline() -> None:
    context.configure(
        url=os.environ["DATABASE_URL"],
        target_metadata=target_metadata,
        version_table=VERSION_TABLE,
        version_table_schema=VERSION_TABLE_SCHEMA,
        include_object=include_object,
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
        connection.execute(text("SELECT pg_advisory_lock(:k)"), {"k": MIGRATION_LOCK_KEY})
        try:
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                version_table=VERSION_TABLE,
                version_table_schema=VERSION_TABLE_SCHEMA,
                include_object=include_object,
            )
            with context.begin_transaction():
                context.run_migrations()
        finally:
            connection.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": MIGRATION_LOCK_KEY})


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
