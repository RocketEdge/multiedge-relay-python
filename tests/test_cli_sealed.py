"""CLI tests for the ``multiedge sealed`` subcommands (keygen/fingerprint/register)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("cryptography", reason="sealed CLI tests need the [sealed] extra")

from fake_relay import API_KEY, FakeRelay, SyncASGITransport

import multiedge_relay.cli as cli_module
from multiedge_relay.cli import main
from multiedge_relay.sealed import RecipientKeypair, SenderKeypair, bundle_fingerprint


def test_sealed_keygen_recipient_writes_file_and_prints_fingerprint(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out_path = tmp_path / "recipient.key.json"

    assert main(["sealed", "keygen", "--kind", "recipient", "--out", str(out_path)]) == 0

    keypair = RecipientKeypair.load(out_path)
    out = capsys.readouterr().out
    assert keypair.display_fingerprint() in out
    assert "recipient" in out


def test_sealed_keygen_sender_dual_by_default_and_no_dual_flag(tmp_path: Path) -> None:
    dual_path = tmp_path / "sender-dual.key.json"
    classical_path = tmp_path / "sender-classical.key.json"

    assert main(["sealed", "keygen", "--kind", "sender", "--out", str(dual_path)]) == 0
    assert (
        main(["sealed", "keygen", "--kind", "sender", "--no-dual", "--out", str(classical_path)])
        == 0
    )

    assert "mldsa65_pub" in SenderKeypair.load(dual_path).public_bundle()
    assert "mldsa65_pub" not in SenderKeypair.load(classical_path).public_bundle()


def test_sealed_keygen_refuses_overwrite(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out_path = tmp_path / "recipient.key.json"
    assert main(["sealed", "keygen", "--kind", "recipient", "--out", str(out_path)]) == 0

    assert main(["sealed", "keygen", "--kind", "recipient", "--out", str(out_path)]) == 1
    assert "exists" in capsys.readouterr().err.lower()


def test_sealed_fingerprint_prints_grouped(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out_path = tmp_path / "sender.key.json"
    keypair = SenderKeypair.generate()
    keypair.save(out_path)

    assert main(["sealed", "fingerprint", "--key", str(out_path)]) == 0

    out = capsys.readouterr().out
    assert keypair.display_fingerprint() in out


def test_sealed_register_posts_recipient_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    relay = FakeRelay()
    monkeypatch.setattr(cli_module, "_build_transport", lambda: SyncASGITransport(relay.app))
    key_path = tmp_path / "recipient.key.json"
    keypair = RecipientKeypair.generate()
    keypair.save(key_path)

    exit_code = main(
        [
            "sealed",
            "register",
            "--key",
            str(key_path),
            "--client",
            "strategy:strat-x",
            "--api-key",
            API_KEY,
        ]
    )

    assert exit_code == 0
    assert relay.recipient_keys["strat-x"][0]["key_id"] == keypair.fingerprint
    assert keypair.fingerprint in capsys.readouterr().out


def test_sealed_register_puts_sender_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    relay = FakeRelay()
    monkeypatch.setattr(cli_module, "_build_transport", lambda: SyncASGITransport(relay.app))
    key_path = tmp_path / "sender.key.json"
    keypair = SenderKeypair.generate()
    keypair.save(key_path)

    exit_code = main(
        [
            "sealed",
            "register",
            "--key",
            str(key_path),
            "--strategy",
            "strat-y",
            "--api-key",
            API_KEY,
        ]
    )

    assert exit_code == 0
    assert relay.sender_keys["strat-y"][0]["key_id"] == keypair.fingerprint


def test_sealed_register_requires_target(tmp_path: Path) -> None:
    key_path = tmp_path / "recipient.key.json"
    RecipientKeypair.generate().save(key_path)

    with pytest.raises(SystemExit):
        main(["sealed", "register", "--key", str(key_path), "--api-key", API_KEY])


def test_sealed_fingerprint_matches_bundle_helper(tmp_path: Path) -> None:
    out_path = tmp_path / "recipient.key.json"
    assert main(["sealed", "keygen", "--kind", "recipient", "--out", str(out_path)]) == 0

    document = json.loads(out_path.read_text(encoding="utf-8"))
    assert document["kid"] == bundle_fingerprint(document["bundle"])
