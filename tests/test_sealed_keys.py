"""Tests for sealed-mode key generation, persistence, and fingerprints.

Purpose:
    ``multiedge_relay.sealed.keys`` owns the hybrid key identities: recipient
    keypairs (X25519 + ML-KEM-768) and sender keypairs (Ed25519 + ML-DSA-65).
    Fingerprints are the trust anchor of sealed mode — they must be stable,
    derived from the canonical public bundle, and recomputable by any party.
"""

from __future__ import annotations

import base64
import json
import os
import stat
import sys
from pathlib import Path

import pytest

pytest.importorskip("cryptography", reason="sealed tests need the [sealed] extra")

from multiedge_relay.sealed.keys import (
    RecipientKeypair,
    SenderKeypair,
    bundle_fingerprint,
    canonical_json,
)


def test_recipient_keypair_roundtrip_save_load(tmp_path: Path) -> None:
    """A generated recipient keypair survives save/load byte-identically."""
    keypair = RecipientKeypair.generate()
    path = tmp_path / "recipient.key.json"
    keypair.save(path)

    loaded = RecipientKeypair.load(path)

    assert loaded.fingerprint == keypair.fingerprint
    assert loaded.public_bundle() == keypair.public_bundle()


def test_sender_keypair_roundtrip_save_load(tmp_path: Path) -> None:
    """A generated dual sender keypair survives save/load byte-identically."""
    keypair = SenderKeypair.generate()
    path = tmp_path / "sender.key.json"
    keypair.save(path)

    loaded = SenderKeypair.load(path)

    assert loaded.fingerprint == keypair.fingerprint
    assert loaded.public_bundle() == keypair.public_bundle()


def test_fingerprint_is_stable_and_matches_bundle() -> None:
    """fingerprint == SHA-256 of the canonical public bundle, 64 lowercase hex."""
    keypair = RecipientKeypair.generate()
    bundle = keypair.public_bundle()

    assert keypair.fingerprint == bundle_fingerprint(bundle)
    assert len(keypair.fingerprint) == 64
    assert keypair.fingerprint == keypair.fingerprint.lower()
    # Stable under dict re-ordering: canonical JSON sorts keys.
    reordered = dict(reversed(list(bundle.items())))
    assert bundle_fingerprint(reordered) == keypair.fingerprint


def test_recipient_bundle_shape() -> None:
    """Recipient bundle carries exactly the documented mek-v1 fields and lengths."""
    bundle = RecipientKeypair.generate().public_bundle()

    assert bundle["bundle"] == "mek-v1"
    assert bundle["purpose"] == "recipient"
    assert len(base64.b64decode(bundle["x25519_pub"])) == 32
    assert len(base64.b64decode(bundle["mlkem768_ek"])) == 1184
    assert set(bundle) == {"bundle", "purpose", "x25519_pub", "mlkem768_ek", "created_at"}


def test_sender_bundle_shape_dual_and_classical_only() -> None:
    """Dual sender bundles carry mldsa65_pub; classical-only bundles omit it."""
    dual = SenderKeypair.generate().public_bundle()
    classical = SenderKeypair.generate(dual=False).public_bundle()

    assert dual["bundle"] == "mek-v1"
    assert dual["purpose"] == "sender"
    assert len(base64.b64decode(dual["ed25519_pub"])) == 32
    assert len(base64.b64decode(dual["mldsa65_pub"])) == 1952
    assert set(dual) == {"bundle", "purpose", "ed25519_pub", "mldsa65_pub", "created_at"}
    assert set(classical) == {"bundle", "purpose", "ed25519_pub", "created_at"}


def test_save_refuses_overwrite(tmp_path: Path) -> None:
    """save() must never clobber an existing key file."""
    keypair = RecipientKeypair.generate()
    path = tmp_path / "recipient.key.json"
    keypair.save(path)

    with pytest.raises(FileExistsError):
        RecipientKeypair.generate().save(path)


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX file modes only")
def test_file_mode_0600_posix(tmp_path: Path) -> None:
    """Key files are created owner-read/write only."""
    path = tmp_path / "recipient.key.json"
    RecipientKeypair.generate().save(path)

    mode = stat.S_IMODE(os.stat(path).st_mode)
    assert mode == 0o600


def test_display_fingerprint_groups_of_four() -> None:
    """Human-facing fingerprint renders as space-separated groups of 4 hex chars."""
    keypair = SenderKeypair.generate()
    shown = keypair.display_fingerprint()

    assert shown.replace(" ", "") == keypair.fingerprint
    assert all(len(group) == 4 for group in shown.split(" "))


def test_canonical_json_is_compact_sorted_ascii() -> None:
    """canonical_json: sorted keys, no whitespace, ASCII-only — the fingerprint input."""
    blob = canonical_json({"b": 1, "a": {"y": "é", "x": [1, 2]}})

    assert blob == b'{"a":{"x":[1,2],"y":"\\u00e9"},"b":1}'
    json.loads(blob)


def test_load_rejects_foreign_json(tmp_path: Path) -> None:
    """Loading a file that is not a sealed key file raises SealedKeyError."""
    from multiedge_relay.exceptions import SealedKeyError

    path = tmp_path / "not-a-key.json"
    path.write_text(json.dumps({"hello": "world"}), encoding="utf-8")

    with pytest.raises(SealedKeyError):
        RecipientKeypair.load(path)
    with pytest.raises(SealedKeyError):
        SenderKeypair.load(path)
