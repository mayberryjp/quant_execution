"""Shared test fixtures.

A ``DATABASE_URL`` is set before importing the application so ``Settings`` can be constructed.
The default points at a database that does not exist, which is fine for tests that only exercise
the readiness *failure* path; tests needing a real database set their own URL / are run in CI.
"""

from __future__ import annotations

import os

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+psycopg://app:app@localhost:5432/quant_execution_test",
)

import pytest
from webtest import TestApp

from quant_execution.api.app import create_app


@pytest.fixture()
def client() -> TestApp:
    return TestApp(create_app())
