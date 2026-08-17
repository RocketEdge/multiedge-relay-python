"""Sealed-mode SDK integration: publisher, subscriber, webhook, key registry.

Purpose:
    End-to-end sealed flows over the in-process fake relay: ciphertext on the
    wire and in the DLQ, plaintext only inside client code; key fetching with
    local fingerprint verification and pinning; unseal failures surface via
    ``on_error`` without committing the cursor (never silent loss).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("cryptography", reason="sealed tests need the [sealed] extra")

from fake_relay import API_KEY, FakeRelay, SyncASGITransport

from multiedge_relay.dlq import DiskDLQ
from multiedge_relay.exceptions import (
    KeyPinningError,
    NotARecipientError,
    PublishFailed,
    SealedKeyError,
)
from multiedge_relay.models import ReceivedSignal, Signal, SignalMeta
from multiedge_relay.publisher import SignalPublisher
from multiedge_relay.publisher_async import AsyncSignalPublisher
from multiedge_relay.sealed import RecipientKeypair, Sealer, SenderKeypair, Unsealer
from multiedge_relay.sealed.registry import register_recipient_key, register_sender_key
from multiedge_relay.subscriber import SignalSubscriber
from multiedge_relay.webhook import verify_signature

STRATEGY = "sealed-strat"


@pytest.fixture(scope="module")
def sender() -> SenderKeypair:
    """Module-scoped dual sender."""
    return SenderKeypair.generate()


@pytest.fixture(scope="module")
def recipient() -> RecipientKeypair:
    """Module-scoped recipient."""
    return RecipientKeypair.generate()


@pytest.fixture()
def sealed_relay(sender: SenderKeypair, recipient: RecipientKeypair) -> FakeRelay:
    """A fake relay pre-seeded with the module keypair bundles for STRATEGY."""
    relay = FakeRelay()
    relay.recipient_keys[STRATEGY] = [
        {
            "key_id": recipient.fingerprint,
            "client_id": "client-1",
            "bundle": recipient.public_bundle(),
        }
    ]
    relay.sender_keys[STRATEGY] = [
        {"key_id": sender.fingerprint, "strategy_id": STRATEGY, "bundle": sender.public_bundle()}
    ]
    return relay


def _sealer(sender: SenderKeypair, recipient: RecipientKeypair) -> Sealer:
    """Direct (no-HTTP) sealer over the module identities."""
    return Sealer(sender=sender, recipients=[recipient.public_bundle()])


def test_publish_with_sealer_sends_ciphertext(
    sealed_relay: FakeRelay, sender: SenderKeypair, recipient: RecipientKeypair
) -> None:
    """The wire body carries a sealed envelope; the relay never sees plaintext keys."""
    publisher = SignalPublisher(
        api_key=API_KEY,
        dlq=None,
        transport=SyncASGITransport(sealed_relay.app),
        sealer=_sealer(sender, recipient),
    )
    with publisher:
        ack = publisher.publish(Signal(strategy_id=STRATEGY, payload={"secret_weight": 0.9}))

    stored = sealed_relay.signals[STRATEGY][0].payload
    assert stored["sealed"] == "v1"
    assert "secret_weight" not in json.dumps(stored)
    assert ack.sequence == 1


def test_async_publisher_seals(
    sealed_relay: FakeRelay, sender: SenderKeypair, recipient: RecipientKeypair
) -> None:
    """The async twin seals identically through the shared prepare_signal path."""
    import asyncio

    import httpx

    async def go() -> None:
        publisher = AsyncSignalPublisher(
            api_key=API_KEY,
            dlq=None,
            transport=httpx.ASGITransport(app=sealed_relay.app),
            sealer=_sealer(sender, recipient),
        )
        try:
            await publisher.publish(Signal(strategy_id=STRATEGY, payload={"secret": True}))
        finally:
            await publisher.aclose()

    asyncio.run(go())

    assert sealed_relay.signals[STRATEGY][0].payload["sealed"] == "v1"


def test_dlq_spill_contains_ciphertext_and_resends_verbatim(
    tmp_path: Path, sender: SenderKeypair, recipient: RecipientKeypair
) -> None:
    """Retry exhaustion spills CIPHERTEXT to disk; a later resend sends identical bytes."""
    relay = FakeRelay()
    relay.fail_next(2, 503)
    dlq = DiskDLQ(root=tmp_path / "dlq")
    publisher = SignalPublisher(
        api_key=API_KEY,
        dlq=dlq,
        max_attempts=2,
        transport=SyncASGITransport(relay.app),
        sealer=_sealer(sender, recipient),
        sleep=lambda _: None,
        random_fn=lambda: 0.0,
    )
    with publisher, pytest.raises(PublishFailed):
        publisher.publish(Signal(strategy_id=STRATEGY, payload={"secret_weight": 0.9}))

    (entry,) = list(dlq.pending())
    assert entry.signal.payload["sealed"] == "v1"
    assert "secret_weight" not in json.dumps(entry.signal.payload)

    with SignalPublisher(
        api_key=API_KEY, dlq=None, transport=SyncASGITransport(relay.app)
    ) as resender:
        report = dlq.resend(resender)

    assert report.resent == 1
    assert relay.signals[STRATEGY][0].payload == entry.signal.payload


def test_sealer_from_relay_fetches_and_verifies_bundles(
    sealed_relay: FakeRelay, sender: SenderKeypair, recipient: RecipientKeypair
) -> None:
    """Sealer.from_relay fetches the recipient set and the result round-trips."""
    sealer = Sealer.from_relay(
        api_key=API_KEY,
        strategy_id=STRATEGY,
        sender=sender,
        transport=SyncASGITransport(sealed_relay.app),
    )
    sealed = sealer.seal_signal(
        Signal(strategy_id=STRATEGY, payload={"p": 1}, client_signal_id="01JCSID000000000000000001")
    )

    unsealer = Unsealer(recipient=recipient, sender_bundle=sender.public_bundle())
    received = ReceivedSignal(
        sequence=1,
        signal_id="sig_x",
        strategy_id=STRATEGY,
        client_signal_id="01JCSID000000000000000001",
        published_at=datetime.now(UTC),
        payload=sealed.payload,
    )
    assert unsealer.unseal_signal(received).payload == {"p": 1}


def test_fingerprint_mismatch_between_kid_and_bundle_raises(
    sender: SenderKeypair, recipient: RecipientKeypair
) -> None:
    """A relay-tampered key_id is caught by local fingerprint recomputation."""
    relay = FakeRelay()
    relay.recipient_keys[STRATEGY] = [
        {"key_id": "0" * 64, "client_id": "client-1", "bundle": recipient.public_bundle()}
    ]

    with pytest.raises(SealedKeyError, match="fingerprint"):
        Sealer.from_relay(
            api_key=API_KEY,
            strategy_id=STRATEGY,
            sender=sender,
            transport=SyncASGITransport(relay.app),
        )


def test_pinning_mismatch_raises_key_pinning_error(
    sealed_relay: FakeRelay, sender: SenderKeypair
) -> None:
    """A pinned recipient set that disagrees with the fetched set is a hard stop."""
    with pytest.raises(KeyPinningError, match="pin"):
        Sealer.from_relay(
            api_key=API_KEY,
            strategy_id=STRATEGY,
            sender=sender,
            pinned_recipients={"f" * 64},
            transport=SyncASGITransport(sealed_relay.app),
        )


def test_unsealer_from_relay_pins_sender_key(
    sealed_relay: FakeRelay, sender: SenderKeypair, recipient: RecipientKeypair
) -> None:
    """Unsealer.from_relay verifies the fetched sender bundle and honors the pin."""
    good = Unsealer.from_relay(
        api_key=API_KEY,
        strategy_id=STRATEGY,
        recipient=recipient,
        pinned_sender=sender.fingerprint,
        transport=SyncASGITransport(sealed_relay.app),
    )
    assert good is not None

    with pytest.raises(KeyPinningError):
        Unsealer.from_relay(
            api_key=API_KEY,
            strategy_id=STRATEGY,
            recipient=recipient,
            pinned_sender="a" * 64,
            transport=SyncASGITransport(sealed_relay.app),
        )


def test_register_helpers_post_bundles(sender: SenderKeypair, recipient: RecipientKeypair) -> None:
    """The register helpers PUT/POST the bundle with the locally computed key_id."""
    relay = FakeRelay()
    register_recipient_key(
        api_key=API_KEY,
        client_id=f"strategy:{STRATEGY}",
        keypair=recipient,
        transport=SyncASGITransport(relay.app),
    )
    register_sender_key(
        api_key=API_KEY,
        strategy_id=STRATEGY,
        keypair=sender,
        transport=SyncASGITransport(relay.app),
    )

    assert relay.recipient_keys[STRATEGY][0]["key_id"] == recipient.fingerprint
    assert relay.sender_keys[STRATEGY][0]["key_id"] == sender.fingerprint


def test_subscriber_delivers_plaintext_when_unsealer_set(
    sealed_relay: FakeRelay, sender: SenderKeypair, recipient: RecipientKeypair
) -> None:
    """Full loop: sealed publish → catch-up → callback sees plaintext, meta intact."""
    with SignalPublisher(
        api_key=API_KEY,
        dlq=None,
        transport=SyncASGITransport(sealed_relay.app),
        sealer=_sealer(sender, recipient),
    ) as publisher:
        publisher.publish(Signal(strategy_id=STRATEGY, payload={"weight": 0.5}))

    seen: list[tuple[ReceivedSignal, SignalMeta]] = []
    subscriber = SignalSubscriber(
        api_key=API_KEY,
        strategy_id=STRATEGY,
        on_signal=lambda signal, meta: seen.append((signal, meta)),
        cursor_store=_MemoryCursor(),
        transport=SyncASGITransport(sealed_relay.app),
        unsealer=Unsealer(recipient=recipient, sender_bundle=sender.public_bundle()),
    )
    delivered = subscriber.catch_up_only()

    assert delivered == 1
    signal, meta = seen[0]
    assert signal.payload == {"weight": 0.5}
    assert meta.source == "catchup"
    assert meta.sequence == 1


def test_unseal_failure_routes_to_on_error_and_does_not_commit_cursor(
    sealed_relay: FakeRelay, sender: SenderKeypair
) -> None:
    """A signal sealed to SOMEONE ELSE fails loudly; the cursor stays uncommitted."""
    outsider = RecipientKeypair.generate()
    with SignalPublisher(
        api_key=API_KEY,
        dlq=None,
        transport=SyncASGITransport(sealed_relay.app),
        sealer=_sealer(sender, RecipientKeypair.generate()),
    ) as publisher:
        publisher.publish(Signal(strategy_id=STRATEGY, payload={"weight": 0.5}))

    errors: list[Exception] = []
    cursor = _MemoryCursor()
    subscriber = SignalSubscriber(
        api_key=API_KEY,
        strategy_id=STRATEGY,
        on_signal=lambda signal, meta: None,
        on_error=errors.append,
        cursor_store=cursor,
        transport=SyncASGITransport(sealed_relay.app),
        unsealer=Unsealer(recipient=outsider, sender_bundle=sender.public_bundle()),
    )

    with pytest.raises(NotARecipientError):
        subscriber.catch_up_only()

    assert len(errors) == 1
    assert cursor.committed == {}


def test_webhook_verify_signature_with_unsealer(
    sender: SenderKeypair, recipient: RecipientKeypair
) -> None:
    """HMAC verification first, then unseal — the callback body ends up plaintext."""
    import base64
    import hashlib
    import hmac as hmac_mod

    sealed = _sealer(sender, recipient).seal_signal(
        Signal(
            strategy_id=STRATEGY,
            payload={"pos": "long"},
            client_signal_id="01JWH0000000000000000001",
        )
    )
    body_dict: dict[str, Any] = {
        "sequence": 5,
        "signal_id": "sig_wh",
        "strategy_id": STRATEGY,
        "client_signal_id": "01JWH0000000000000000001",
        "published_at": datetime.now(UTC).isoformat(),
        "payload": sealed.payload,
    }
    raw_body = json.dumps(body_dict).encode()
    secret_bytes = b"\x07" * 32
    secret = base64.b64encode(secret_bytes).decode()
    timestamp = int(datetime.now(UTC).timestamp())
    digest = hmac_mod.new(secret_bytes, f"{timestamp}.".encode() + raw_body, hashlib.sha256)

    received = verify_signature(
        raw_body,
        {
            "X-MultiEdge-Signature": f"sha256={digest.hexdigest()}",
            "X-MultiEdge-Timestamp": str(timestamp),
        },
        secret,
        unsealer=Unsealer(recipient=recipient, sender_bundle=sender.public_bundle()),
    )

    assert received.payload == {"pos": "long"}
    assert received.sequence == 5


class _MemoryCursor:
    """Minimal in-memory CursorStore for the tests."""

    def __init__(self) -> None:
        self.committed: dict[str, int] = {}

    def load(self, strategy_id: str) -> int | None:
        """Return the committed sequence, if any."""
        return self.committed.get(strategy_id)

    def commit(self, strategy_id: str, sequence: int) -> None:
        """Persist the cursor in memory."""
        self.committed[strategy_id] = sequence
