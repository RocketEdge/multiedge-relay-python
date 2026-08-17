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

from ..exceptions import SealedError, UnsealError
from ..models import ReceivedSignal, Signal
from .core import seal, unseal
from .keys import RecipientKeypair, SenderKeypair

__all__ = ["Sealer", "Unsealer"]


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
