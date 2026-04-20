"""Shared pytest fixtures for the YHODA pipeline test suite.

All fixtures that need to be available across unit and integration tests
should be defined here.
"""

from __future__ import annotations

import pytest

from yhovi_pipeline.config import Settings, get_settings


@pytest.fixture(autouse=True, scope="session")
def _suppress_prefect_api_log_warning(tmp_path_factory: pytest.TempPathFactory) -> None:
    """Silence the Prefect 'no flow run id' UserWarning emitted when tasks
    are called directly outside a flow context in unit tests.
    """
    import os

    os.environ.setdefault("PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW", "ignore")


@pytest.fixture(autouse=False)
def test_settings(monkeypatch: pytest.MonkeyPatch) -> Settings:
    """Return a ``Settings`` instance suitable for testing.

    Uses dummy values for required secrets so tests can run without a real
    ``.env`` file or exported environment variables.  Clears the
    ``get_settings`` LRU cache before and after each test to ensure
    isolation.

    Usage::

        def test_something(test_settings: Settings) -> None:
            assert test_settings.log_level == "DEBUG"
    """
    # Clear the cache so the monkeypatched env is picked up.
    get_settings.cache_clear()

    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+psycopg2://test:test@localhost/test_db",
    )
    monkeypatch.setenv("DWP_API_KEY", "test-dwp-key")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    settings = get_settings()

    yield settings

    # Clear cache again after the test so subsequent tests start fresh.
    get_settings.cache_clear()
