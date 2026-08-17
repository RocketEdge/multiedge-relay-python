"""Tests for the ``multiedge_relay.sealed`` optional-extra import guard.

Purpose:
    The sealed-mode crypto lives behind the ``multiedge-relay[sealed]`` extra so the
    core SDK keeps its httpx+pydantic-only dependency contract. Importing the
    subpackage without ``cryptography`` installed must fail fast with an actionable
    message, mirroring the ``[webpubsub]`` guard in ``subscriber.py``.
"""

from __future__ import annotations

import builtins
import importlib
import sys
from typing import Any

import pytest


def test_import_without_cryptography_raises_friendly_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Simulate a missing ``cryptography`` wheel and assert the install hint."""
    for name in [
        m
        for m in sys.modules
        if m == "multiedge_relay.sealed" or m.startswith("multiedge_relay.sealed.")
    ]:
        monkeypatch.delitem(sys.modules, name)
    for name in [m for m in sys.modules if m == "cryptography" or m.startswith("cryptography.")]:
        monkeypatch.delitem(sys.modules, name)

    real_import = builtins.__import__

    def blocking_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "cryptography" or name.startswith("cryptography."):
            raise ImportError(f"No module named {name!r}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocking_import)

    with pytest.raises(ImportError, match=r"multiedge-relay\[sealed\]"):
        importlib.import_module("multiedge_relay.sealed")


def test_core_package_does_not_import_sealed() -> None:
    """Importing the core package must not pull in the sealed subpackage.

    The httpx+pydantic-only rule for core: ``multiedge_relay`` stays importable
    and fully functional without the ``[sealed]`` extra installed.
    """
    saved = {
        name: module
        for name, module in sys.modules.items()
        if name == "multiedge_relay" or name.startswith("multiedge_relay.")
    }
    for name in saved:
        del sys.modules[name]
    try:
        importlib.import_module("multiedge_relay")
        assert "multiedge_relay.sealed" not in sys.modules
    finally:
        # Restore the original module objects: leaving freshly re-imported
        # modules in sys.modules would give later tests different exception
        # class identities than the modules they imported at collection time.
        for name in [
            m for m in sys.modules if m == "multiedge_relay" or m.startswith("multiedge_relay.")
        ]:
            del sys.modules[name]
        sys.modules.update(saved)


def test_sealed_exceptions_live_in_core() -> None:
    """The sealed exception taxonomy is stdlib-only and importable without the extra."""
    from multiedge_relay.exceptions import (
        KeyPinningError,
        MultiEdgeError,
        NotARecipientError,
        SealedError,
        SealedKeyError,
        UnsealError,
    )

    assert issubclass(SealedError, MultiEdgeError)
    assert issubclass(UnsealError, SealedError)
    assert issubclass(SealedKeyError, SealedError)
    assert issubclass(KeyPinningError, SealedError)
    assert issubclass(NotARecipientError, UnsealError)
