"""Contract test binding the two hand-edited version strings together.

Purpose:
    The package version is written twice by hand — ``[project].version`` in
    ``pyproject.toml`` (what the wheel is named and what PyPI records) and
    ``multiedge_relay.__version__`` (what a caller reads at runtime, and what the
    README tells them to check). Nothing derives one from the other, so a release
    that bumps only one ships a distribution whose self-reported version is a lie.

    That defect is invisible to every other gate: the build succeeds, the upload
    succeeds, the tests pass, and the wrong number only surfaces in a support
    thread. The release workflow guards the git tag against ``pyproject.toml``
    (``.github/workflows/release.yml``); this guards the runtime constant against
    the same source of truth, closing the other half of the same gap.

Contract:
    Reads both files from disk. No network, no import side effects beyond
    importing the package itself. Fails loudly with both values on drift.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

import multiedge_relay

PYPROJECT = Path(__file__).resolve().parent.parent / "pyproject.toml"


def _packaged_version() -> str:
    """Return ``[project].version`` as declared in ``pyproject.toml``.

    Returns:
        The static version string hatchling will stamp onto the built
        distribution.

    Raises:
        KeyError: if the ``[project].version`` key is absent, which would mean
            the project moved to a dynamic version and this test needs rewriting
            rather than deleting.
    """
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    version: str = data["project"]["version"]
    return version


def test_runtime_version_matches_pyproject_version() -> None:
    packaged = _packaged_version()
    assert multiedge_relay.__version__ == packaged, (
        f"multiedge_relay.__version__ is {multiedge_relay.__version__!r} but "
        f"pyproject.toml declares {packaged!r} — bump both, or the published "
        f"wheel reports a version it is not."
    )
