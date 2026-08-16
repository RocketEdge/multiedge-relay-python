"""Shared fixtures: fake relay, transports, and DLQ/cursor stores rooted in tmp_path."""

from __future__ import annotations

from pathlib import Path

import pytest
from fake_relay import API_KEY, FakeRelay, SyncASGITransport

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def relay() -> FakeRelay:
    return FakeRelay()


@pytest.fixture
def relay_transport(relay: FakeRelay) -> SyncASGITransport:
    return SyncASGITransport(relay.app)


@pytest.fixture
def api_key() -> str:
    return API_KEY


@pytest.fixture
def dlq_root(tmp_path: Path) -> Path:
    return tmp_path / "dlq"


@pytest.fixture
def cursor_root(tmp_path: Path) -> Path:
    return tmp_path / "cursor"
