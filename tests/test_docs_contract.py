"""Contract tests over shipped TEXT: placeholder credentials and example payloads.

Purpose:
    Documentation is part of the product surface. Two classes of defect are invisible
    to every other test because they live in prose and example literals:

    * A placeholder API key with the wrong prefix. The relay accepts only ``mesk_``
      keys, so a copy-pasted quickstart is rejected at auth before the reader reaches
      anything the SDK does.
    * An example payload the relay's own standard schema would refuse. A new feed
      defaults to ``portfolio_rebalance/1.0``, which is ``additionalProperties:
      false`` — so a single-ticker payload is a guaranteed 422 against the very feed
      the reader just created.

    Both are caught here by reading the shipped files, not by mocking anything.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).parent.parent
EXAMPLES = REPO_ROOT / "examples"

#: The only valid API-key prefix; see the relay's ApiKeyCallerResolver.
VALID_KEY_PREFIX = "mesk_"

#: The prefix that was shipped by mistake — never valid, and rejected at auth.
INVALID_KEY_PREFIX = "mek_"

#: Required top-level keys of portfolio_rebalance/1.0.
SCHEMA_REQUIRED = ("kind", "signal_date", "planned_execution_date", "positions")

#: Required keys of each entry in ``positions``.
POSITION_REQUIRED = ("ticker", "action", "signal_portfolio_weight")

#: The schema's ``action`` enum.
POSITION_ACTIONS = frozenset({"INITIALIZE", "BUY", "SELL"})


def shipped_text_files() -> list[Path]:
    """Every file whose text a reader copies from: the README, examples, and the package.

    Returns:
        Paths that must never contain an invalid credential placeholder.
    """
    files = [REPO_ROOT / "README.md"]
    files.extend(sorted(EXAMPLES.rglob("*.py")))
    files.extend(sorted((REPO_ROOT / "src" / "multiedge_relay").rglob("*.py")))
    return files


# ------------------------------------------------------------------- key placeholders
def test_no_invalid_api_key_prefix_is_shipped() -> None:
    offenders = [
        path.relative_to(REPO_ROOT).as_posix()
        for path in shipped_text_files()
        # Match the bad prefix only where it is NOT the tail of the good one.
        if INVALID_KEY_PREFIX in path.read_text(encoding="utf-8").replace(VALID_KEY_PREFIX, "")
    ]
    assert offenders == [], (
        f"{INVALID_KEY_PREFIX!r} is not a valid key prefix — the relay accepts only "
        f"{VALID_KEY_PREFIX!r}, so these copy-pasted samples fail at auth: {offenders}"
    )


# ------------------------------------------------------------------- example payloads
@pytest.fixture(scope="module")
def publish_minimal() -> Any:
    sys.path.insert(0, str(EXAMPLES))
    try:
        return importlib.import_module("publish_minimal")
    finally:
        sys.path.remove(str(EXAMPLES))


def test_no_shipped_sample_uses_a_field_the_standard_schema_rejects() -> None:
    """No reader-facing sample may name a field `portfolio_rebalance/1.0` refuses.

    The schema is ``additionalProperties: false``, so `target_weight` is not a
    harmless synonym for `signal_portfolio_weight` — it is a 422. This checks the
    shipped TEXT because that is what people copy; the previous version of this
    test pinned one example's payload and three other samples drifted past it,
    including the README quickstart that PyPI renders as the project page.
    """
    offenders = [
        path.relative_to(REPO_ROOT).as_posix()
        for path in shipped_text_files()
        if "target_weight"
        in path.read_text(encoding="utf-8").replace(
            # The prose that warns against the field necessarily names it.
            "not `target_weight`",
            "",
        )
    ]
    assert offenders == [], (
        "portfolio_rebalance/1.0 is additionalProperties:false — `target_weight` is "
        f"refused with 422, so these samples cannot be copied: {offenders}"
    )


def test_minimal_example_payload_matches_the_standard_schema(publish_minimal: Any) -> None:
    payload = publish_minimal.rebalance_payload()

    assert set(SCHEMA_REQUIRED) <= set(payload), (
        "the minimal example must satisfy portfolio_rebalance/1.0 — a new feed defaults "
        "to that schema, and additionalProperties:false makes anything else a 422"
    )
    assert payload["kind"] == "portfolio_rebalance"
    assert payload["positions"], "an empty positions list is a heartbeat, not an example"

    for position in payload["positions"]:
        assert set(position) == set(POSITION_REQUIRED)
        assert position["action"] in POSITION_ACTIONS
        assert 0 <= position["signal_portfolio_weight"] <= 1
