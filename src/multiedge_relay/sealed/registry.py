"""Sealer/Unsealer: sealed mode at the SDK model layer, plus key distribution.

Purpose:
    Connects the pure crypto core to the SDK's frozen pydantic models.
    ``Sealer`` turns a plaintext ``Signal`` into a sealed one (used by the
    publishers via ``prepare_signal``); ``Unsealer`` turns a sealed
    ``ReceivedSignal`` back into plaintext (used by the subscriber's delivery
    funnel and by ``verify_signature``).

Contract:
    * Both classes are immutable after construction and hold key material for
      exactly one strategy relationship.
    * Trust model: bundles obtained from the relay are ALWAYS re-fingerprinted
      locally (the relay is untrusted for key authenticity); pinning against
      out-of-band fingerprints is the strongest configuration.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import httpx

from .._http import DEFAULT_BASE_URL, build_client
from ..exceptions import KeyPinningError, SealedError, SealedKeyError, UnsealError
from ..models import ReceivedSignal, Signal
from .core import seal, unseal
from .keys import RecipientKeypair, SenderKeypair, bundle_fingerprint

__all__ = ["Sealer", "Unsealer", "register_recipient_key", "register_sender_key"]


def _fetch_verified_bundles(client: httpx.Client, path: str, what: str) -> list[dict[str, Any]]:
    """GET a key listing and verify every fingerprint locally.

    The relay is UNTRUSTED for key authenticity: each entry's ``key_id`` must
    equal the locally recomputed fingerprint of its bundle, or the whole fetch
    is rejected — a mismatch means transport corruption or a tampering relay.

    Args:
        client: Authenticated httpx client.
        path: Listing path (``.../sealed-keys`` or ``.../sealed-keys/sender``).
        what: Human label for error messages ("recipient"/"sender").

    Returns:
        The verified bundle dicts, in server order.

    Raises:
        SealedKeyError: HTTP failure or a fingerprint mismatch.
    """
    response = client.get(path)
    if response.status_code != 200:
        raise SealedKeyError(
            f"fetching {what} keys failed (HTTP {response.status_code}): {response.text[:200]}"
        )
    bundles: list[dict[str, Any]] = []
    for entry in response.json().get("keys", []):
        bundle = entry.get("bundle")
        key_id = entry.get("key_id")
        if not isinstance(bundle, dict) or bundle_fingerprint(bundle) != key_id:
            raise SealedKeyError(
                f"{what} key {str(key_id)[:16]}…: bundle fingerprint mismatch — the "
                "relay served a bundle that does not hash to its key_id; do not trust it"
            )
        bundles.append(bundle)
    return bundles


def register_recipient_key(
    *,
    api_key: str,
    client_id: str,
    keypair: RecipientKeypair,
    base_url: str = DEFAULT_BASE_URL,
    transport: httpx.BaseTransport | None = None,
) -> None:
    """Register a recipient public bundle with the relay.

    Args:
        api_key: Subscriber (or admin) API key.
        client_id: The client the key belongs to.
        keypair: The keypair whose PUBLIC bundle to register (the private half
            never leaves this process).
        base_url: Relay origin.
        transport: httpx transport override (test seam).

    Raises:
        SealedKeyError: On any non-2xx response.
    """
    with build_client(api_key, base_url=base_url, transport=transport) as client:
        response = client.post(
            f"/v1/clients/{client_id}/sealed-keys",
            json={"key_id": keypair.fingerprint, "bundle": keypair.public_bundle()},
        )
        if response.status_code not in (200, 201):
            raise SealedKeyError(
                f"recipient key registration failed (HTTP {response.status_code}): "
                f"{response.text[:200]}"
            )


def register_sender_key(
    *,
    api_key: str,
    strategy_id: str,
    keypair: SenderKeypair,
    base_url: str = DEFAULT_BASE_URL,
    transport: httpx.BaseTransport | None = None,
) -> None:
    """Register a sender public bundle with the relay.

    Args:
        api_key: Publisher (or admin) API key.
        strategy_id: The strategy the signing key belongs to.
        keypair: The keypair whose PUBLIC bundle to register.
        base_url: Relay origin.
        transport: httpx transport override (test seam).

    Raises:
        SealedKeyError: On any non-2xx response.
    """
    with build_client(api_key, base_url=base_url, transport=transport) as client:
        response = client.put(
            f"/v1/strategies/{strategy_id}/sealed-keys/sender",
            json={"key_id": keypair.fingerprint, "bundle": keypair.public_bundle()},
        )
        if response.status_code not in (200, 201):
            raise SealedKeyError(
                f"sender key registration failed (HTTP {response.status_code}): "
                f"{response.text[:200]}"
            )


class Sealer:
    """Seals outbound ``Signal`` payloads for one strategy's recipient set.

    Purpose:
        Held by a publisher; applied inside ``prepare_signal`` after the
        idempotency ULID is assigned and before any DLQ spill, so the DLQ
        stores ciphertext and resends are byte-identical.

    Contract:
        The recipient set is fixed at construction (fetch-at-construction in
        v1): subscribers entitled after construction are not sealed to until a
        new ``Sealer`` is built.
    """

    def __init__(
        self,
        *,
        sender: SenderKeypair,
        recipients: Sequence[dict[str, Any]],
    ) -> None:
        """Bind the sender identity and recipient bundles.

        Args:
            sender: The publisher's signing keypair.
            recipients: Public recipient bundles to seal every signal to.

        Raises:
            SealedError: When ``recipients`` is empty.
        """
        if not recipients:
            raise SealedError(
                "Sealer needs at least one recipient bundle — no entitled "
                "subscriber has registered a sealed key yet"
            )
        self._sender = sender
        self._recipients = [dict(bundle) for bundle in recipients]

    @classmethod
    def from_relay(
        cls,
        *,
        api_key: str,
        strategy_id: str,
        sender: SenderKeypair,
        base_url: str = DEFAULT_BASE_URL,
        pinned_recipients: set[str] | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> Sealer:
        """Build a Sealer from the relay's current entitled recipient set.

        Every fetched bundle's fingerprint is recomputed locally and must match
        its ``key_id``. The set is fetched ONCE — subscribers entitled after
        construction are not sealed to until a new Sealer is built.

        Args:
            api_key: Publisher (or admin) API key.
            strategy_id: The sealed strategy to publish to.
            sender: The publisher's signing keypair.
            base_url: Relay origin.
            pinned_recipients: Optional out-of-band-verified fingerprint set;
                when given, the fetched set must EQUAL it exactly.
            transport: httpx transport override (test seam).

        Raises:
            SealedKeyError: HTTP failure or a fingerprint mismatch.
            KeyPinningError: The fetched set differs from ``pinned_recipients``.
            SealedError: The relay returned no recipient keys.
        """
        with build_client(api_key, base_url=base_url, transport=transport) as client:
            bundles = _fetch_verified_bundles(
                client, f"/v1/strategies/{strategy_id}/sealed-keys", "recipient"
            )
        if pinned_recipients is not None:
            fetched = {bundle_fingerprint(bundle) for bundle in bundles}
            if fetched != pinned_recipients:
                unexpected = sorted(fetched - pinned_recipients)
                missing = sorted(pinned_recipients - fetched)
                raise KeyPinningError(
                    "recipient set does not match the pin: "
                    f"unexpected={unexpected} missing={missing} — confirm fingerprints "
                    "out-of-band before updating the pin set"
                )
        return cls(sender=sender, recipients=bundles)

    def seal_signal(self, signal: Signal) -> Signal:
        """Return a copy of ``signal`` with its payload sealed.

        Args:
            signal: The plaintext signal; ``client_signal_id`` must be set
                (``prepare_signal`` assigns it before sealing).

        Returns:
            A new frozen ``Signal`` whose payload is the sealed envelope.

        Raises:
            SealedError: When ``client_signal_id`` is missing.
        """
        if not signal.client_signal_id:
            raise SealedError(
                "seal_signal requires signal.client_signal_id — sealing binds the "
                "envelope to the idempotency key (use prepare_signal, which assigns it)"
            )
        envelope = seal(
            signal.payload,
            strategy_id=signal.strategy_id,
            client_signal_id=signal.client_signal_id,
            recipients=self._recipients,
            sender=self._sender,
        )
        return signal.model_copy(update={"payload": envelope})


class Unsealer:
    """Unseals inbound ``ReceivedSignal`` payloads from one pinned sender.

    Purpose:
        Held by a subscriber; applied in the delivery funnel (and in
        ``verify_signature`` for webhooks) so callbacks always see plaintext.

    Contract:
        The sender bundle is pinned at construction; every unseal verifies the
        envelope's ``sender_kid`` and signatures against it — the relay cannot
        substitute a sender after construction.
    """

    def __init__(
        self,
        *,
        recipient: RecipientKeypair,
        sender_bundle: dict[str, Any],
    ) -> None:
        """Bind the subscriber keypair and the pinned sender bundle.

        Args:
            recipient: This subscriber's hybrid keypair.
            sender_bundle: The publisher's public bundle (verify its
                fingerprint out-of-band before trusting it).
        """
        self._recipient = recipient
        self._sender_bundle = dict(sender_bundle)

    @classmethod
    def from_relay(
        cls,
        *,
        api_key: str,
        strategy_id: str,
        recipient: RecipientKeypair,
        base_url: str = DEFAULT_BASE_URL,
        pinned_sender: str | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> Unsealer:
        """Build an Unsealer pinned to the strategy's newest sender bundle.

        The fetched bundle's fingerprint is recomputed locally; passing
        ``pinned_sender`` (the fingerprint confirmed out-of-band with the
        publisher) removes the relay from the trust path entirely.

        Args:
            api_key: Subscriber API key (must hold an active entitlement).
            strategy_id: The sealed strategy subscribed to.
            recipient: This subscriber's keypair.
            base_url: Relay origin.
            pinned_sender: Optional out-of-band-verified sender fingerprint.
            transport: httpx transport override (test seam).

        Raises:
            SealedKeyError: HTTP failure, no sender key, or fingerprint mismatch.
            KeyPinningError: The newest sender bundle differs from the pin.
        """
        with build_client(api_key, base_url=base_url, transport=transport) as client:
            bundles = _fetch_verified_bundles(
                client, f"/v1/strategies/{strategy_id}/sealed-keys/sender", "sender"
            )
        if not bundles:
            raise SealedKeyError(
                f"strategy {strategy_id!r} has no registered sender key — the publisher "
                "must run `multiedge sealed register` first"
            )
        newest = bundles[0]
        if pinned_sender is not None and bundle_fingerprint(newest) != pinned_sender:
            raise KeyPinningError(
                f"sender key {bundle_fingerprint(newest)[:16]}… does not match the pinned "
                f"fingerprint {pinned_sender[:16]}… — confirm with the publisher out-of-band"
            )
        return cls(recipient=recipient, sender_bundle=newest)

    def unseal_signal(self, received: ReceivedSignal) -> ReceivedSignal:
        """Return a copy of ``received`` with its payload decrypted.

        Args:
            received: The sealed received signal; must carry
                ``client_signal_id`` (the relay envelope always echoes it).

        Returns:
            A new frozen ``ReceivedSignal`` with the plaintext payload.

        Raises:
            UnsealError: On any verification/decryption failure, or when
                ``client_signal_id`` is absent.
            NotARecipientError: When this key is not in the recipient list.
        """
        if not received.client_signal_id:
            raise UnsealError(
                "received signal lacks client_signal_id — required to reconstruct "
                "the sealed envelope's identity binding (relay envelopes echo it; "
                "webhook/WS bodies always include it)"
            )
        plaintext = unseal(
            received.payload,
            strategy_id=received.strategy_id,
            client_signal_id=received.client_signal_id,
            recipient=self._recipient,
            sender_bundle=self._sender_bundle,
        )
        return received.model_copy(update={"payload": plaintext})
