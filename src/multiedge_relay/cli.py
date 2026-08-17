"""``multiedge`` command-line interface: DLQ recovery and cursor management.

Purpose:
    Operator tooling for the two durable stores the SDK writes:

    * ``multiedge dlq list|resend [--dry-run]|purge`` — inspect and recover
      dead-lettered signals.
    * ``multiedge cursor show|reset --strategy X --to N`` — inspect and explicitly
      move subscriber cursors (reset always requires ``--to``; there is no implicit
      reset, matching the never-silently-reset contract).
    * ``multiedge sealed keygen|fingerprint|register`` — sealed-mode key
      management (requires the ``[sealed]`` extra; imported lazily so the rest
      of the CLI works without it).

Contract:
    ``main(argv)`` returns a process exit code (0 = success) and never raises for
    expected operational failures — it prints them. Credentials for ``dlq resend``
    come from ``--api-key``/``--base-url`` or ``MULTIEDGE_API_KEY``/
    ``MULTIEDGE_BASE_URL``.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

import httpx

from ._http import DEFAULT_BASE_URL
from .cursor import FileCursorStore
from .dlq import DiskDLQ
from .exceptions import CursorCorruptError, MultiEdgeError, SealedError
from .publisher import SignalPublisher


def _build_transport() -> httpx.BaseTransport | None:
    """Transport factory for ``dlq resend`` — patched by tests to stay in-process."""
    return None


def _build_parser() -> argparse.ArgumentParser:
    """Construct the argparse tree for the ``multiedge`` command."""
    parser = argparse.ArgumentParser(
        prog="multiedge", description="MultiEdge Signal Relay SDK utilities"
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    dlq = subcommands.add_parser("dlq", help="dead-letter queue operations")
    dlq_sub = dlq.add_subparsers(dest="dlq_command", required=True)
    for name, help_text in (
        ("list", "list pending dead-lettered signals"),
        ("resend", "re-publish pending signals; successes are removed"),
        ("purge", "delete all pending entries (explicit data loss)"),
    ):
        sub = dlq_sub.add_parser(name, help=help_text)
        sub.add_argument("--strategy", help="restrict to one strategy id")
        sub.add_argument("--root", type=Path, help="DLQ root (default ~/.multiedge/dlq)")
        if name == "resend":
            sub.add_argument("--dry-run", action="store_true", help="only report, send nothing")
            sub.add_argument("--api-key", help="relay API key (or MULTIEDGE_API_KEY)")
            sub.add_argument("--base-url", help="relay origin (or MULTIEDGE_BASE_URL)")

    sealed = subcommands.add_parser("sealed", help="sealed-mode (E2E encryption) key operations")
    sealed_sub = sealed.add_subparsers(dest="sealed_command", required=True)
    keygen = sealed_sub.add_parser("keygen", help="generate a keypair file (never overwrites)")
    keygen.add_argument(
        "--kind",
        choices=("recipient", "sender"),
        required=True,
        help="recipient = subscriber decryption keys; sender = publisher signing keys",
    )
    keygen.add_argument("--out", type=Path, required=True, help="key file path (created 0600)")
    keygen.add_argument(
        "--no-dual",
        action="store_true",
        help="sender only: omit the ML-DSA-65 post-quantum signature key (NOT recommended)",
    )
    fingerprint = sealed_sub.add_parser(
        "fingerprint", help="print a key file's fingerprint for out-of-band verification"
    )
    fingerprint.add_argument("--key", type=Path, required=True, help="key file path")
    register = sealed_sub.add_parser(
        "register", help="register the PUBLIC bundle of a key file with the relay"
    )
    register.add_argument("--key", type=Path, required=True, help="key file path")
    target = register.add_mutually_exclusive_group(required=True)
    target.add_argument("--client", help="client id (recipient keys)")
    target.add_argument("--strategy", help="strategy id (sender keys)")
    register.add_argument("--api-key", help="relay API key (or MULTIEDGE_API_KEY)")
    register.add_argument("--base-url", help="relay origin (or MULTIEDGE_BASE_URL)")

    cursor = subcommands.add_parser("cursor", help="subscriber cursor operations")
    cursor_sub = cursor.add_subparsers(dest="cursor_command", required=True)
    show = cursor_sub.add_parser("show", help="show persisted cursors")
    show.add_argument("--strategy", help="restrict to one strategy id")
    show.add_argument("--root", type=Path, help="cursor root (default ~/.multiedge/cursor)")
    reset = cursor_sub.add_parser("reset", help="explicitly move a cursor")
    reset.add_argument("--strategy", required=True, help="strategy id to reset")
    reset.add_argument("--to", type=int, required=True, help="sequence to set the cursor to")
    reset.add_argument("--root", type=Path, help="cursor root (default ~/.multiedge/cursor)")

    return parser


def _cmd_dlq_list(dlq: DiskDLQ, strategy: str | None) -> int:
    """Print pending DLQ entries; returns 0."""
    entries = list(dlq.pending(strategy))
    if not entries:
        print("DLQ: no pending entries.")
        return 0
    print(f"DLQ: {len(entries)} pending entrie(s):")
    for entry in entries:
        print(
            f"  {entry.failed_at.isoformat()}  strategy={entry.signal.strategy_id}  "
            f"client_signal_id={entry.signal.client_signal_id}  "
            f"attempts={entry.attempts}  error={entry.error}  file={entry.path}"
        )
    return 0


def _cmd_dlq_resend(dlq: DiskDLQ, args: argparse.Namespace) -> int:
    """Resend pending entries via a publisher; returns 0 unless resends failed."""
    if args.dry_run:
        report = dlq.resend(SignalPublisher(api_key="dry-run-unused", dlq=None), dry_run=True)
        print(f"DLQ dry run: {report.attempted} entrie(s) would be resent.")
        return 0
    api_key = args.api_key or os.environ.get("MULTIEDGE_API_KEY")
    if not api_key:
        print(
            "error: an API key is required — pass --api-key or set MULTIEDGE_API_KEY",
            file=sys.stderr,
        )
        return 2
    base_url = args.base_url or os.environ.get("MULTIEDGE_BASE_URL") or DEFAULT_BASE_URL
    with SignalPublisher(
        api_key=api_key, base_url=base_url, dlq=dlq, transport=_build_transport()
    ) as publisher:
        report = dlq.resend(publisher)
    print(
        f"DLQ resend: {report.resent} resent, {report.failed} failed "
        f"(of {report.attempted} attempted)."
    )
    return 0 if report.failed == 0 else 1


def _cmd_cursor_show(store: FileCursorStore, strategy: str | None) -> int:
    """Print persisted cursors; returns 0 (corrupt files are reported, not hidden)."""
    if strategy is not None:
        paths = [store.path_for(strategy)]
    else:
        paths = sorted(store.root.glob("*.json")) if store.root.is_dir() else []
    if not all(p.is_file() for p in paths) or not paths:
        print("No cursors found.")
        return 0
    for path in paths:
        strategy_id = path.stem
        try:
            sequence = store.load(strategy_id)
        except CursorCorruptError as exc:
            print(f"  {strategy_id}: CORRUPT ({exc})")
            continue
        print(f"  {strategy_id}: sequence={sequence}  file={path}")
    return 0


def _cmd_cursor_reset(store: FileCursorStore, strategy: str, to: int) -> int:
    """Explicitly move a cursor, printing old -> new; returns 0."""
    try:
        old: int | str | None = store.load(strategy)
    except CursorCorruptError:
        old = "CORRUPT"
    store.commit(strategy, to)
    print(f"cursor for {strategy!r}: {old} -> {to}")
    return 0


def _cmd_sealed(args: argparse.Namespace) -> int:
    """Dispatch the ``sealed`` subcommands; lazy-imports the crypto extra.

    Returns:
        0 on success, 1 on operational failures (missing extra, existing file,
        registration errors), 2 on missing credentials.
    """
    try:
        from . import sealed as sealed_module
    except ImportError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.sealed_command == "keygen":
        try:
            if args.kind == "recipient":
                keypair: Any = sealed_module.RecipientKeypair.generate()
            else:
                keypair = sealed_module.SenderKeypair.generate(dual=not args.no_dual)
            keypair.save(args.out)
        except FileExistsError:
            print(f"error: {args.out} already exists — never overwritten", file=sys.stderr)
            return 1
        print(f"{args.kind} key written to {args.out}")
        print(f"fingerprint: {keypair.display_fingerprint()}")
        print("Share the fingerprint over a separate channel so peers can verify the bundle.")
        return 0

    if args.sealed_command == "fingerprint":
        for loader in (sealed_module.RecipientKeypair, sealed_module.SenderKeypair):
            try:
                loaded = loader.load(args.key)
            except SealedError:
                continue
            print(f"fingerprint: {loaded.display_fingerprint()}")
            return 0
        print(f"error: {args.key} is not a sealed key file", file=sys.stderr)
        return 1

    # register
    api_key = args.api_key or os.environ.get("MULTIEDGE_API_KEY")
    if not api_key:
        print(
            "error: an API key is required — pass --api-key or set MULTIEDGE_API_KEY",
            file=sys.stderr,
        )
        return 2
    base_url = args.base_url or os.environ.get("MULTIEDGE_BASE_URL") or DEFAULT_BASE_URL
    transport = _build_transport()
    try:
        if args.client is not None:
            recipient = sealed_module.RecipientKeypair.load(args.key)
            sealed_module.registry.register_recipient_key(
                api_key=api_key,
                client_id=args.client,
                keypair=recipient,
                base_url=base_url,
                transport=transport,
            )
            print(f"recipient key registered for client {args.client}")
            print(f"fingerprint: {recipient.fingerprint}")
        else:
            sender = sealed_module.SenderKeypair.load(args.key)
            sealed_module.registry.register_sender_key(
                api_key=api_key,
                strategy_id=args.strategy,
                keypair=sender,
                base_url=base_url,
                transport=transport,
            )
            print(f"sender key registered for strategy {args.strategy}")
            print(f"fingerprint: {sender.fingerprint}")
    except SealedError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Args:
        argv: Argument list (defaults to ``sys.argv[1:]``); passing it directly is
            the test seam.

    Returns:
        Process exit code: 0 on success, 1 on failed resends, 2 on usage errors.
    """
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "dlq":
            dlq = DiskDLQ(root=args.root)
            if args.dlq_command == "list":
                return _cmd_dlq_list(dlq, args.strategy)
            if args.dlq_command == "resend":
                return _cmd_dlq_resend(dlq, args)
            if args.dlq_command == "purge":
                removed = dlq.purge(args.strategy)
                print(f"DLQ purge: removed {removed} entrie(s).")
                return 0
        if args.command == "sealed":
            return _cmd_sealed(args)
        if args.command == "cursor":
            store = FileCursorStore(root=args.root)
            if args.cursor_command == "show":
                return _cmd_cursor_show(store, args.strategy)
            if args.cursor_command == "reset":
                return _cmd_cursor_reset(store, args.strategy, args.to)
    except MultiEdgeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    raise AssertionError("unreachable: argparse enforces a valid subcommand")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
