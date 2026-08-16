# multiedge-relay

Official Python SDK for MultiEdge Signal Relay — auditable signal-distribution infrastructure, not execution.

[![PyPI](https://img.shields.io/pypi/v/multiedge-relay.svg)](https://pypi.org/project/multiedge-relay/)
[![Python](https://img.shields.io/pypi/pyversions/multiedge-relay.svg)](https://pypi.org/project/multiedge-relay/)
[![CI](https://img.shields.io/badge/CI-passing-brightgreen)](https://github.com/multiedge-ai/multiedge-relay-python/actions)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

## Table of Contents

- [Overview](#overview)
- [Install](#install)
- [Quickstart: Publish](#quickstart-publish)
- [Quickstart: Subscribe](#quickstart-subscribe)
- [Webhook Verification](#webhook-verification)
- [Error Handling — Never Silent Loss](#error-handling--never-silent-loss)
- [Dead-Letter Queue CLI](#dead-letter-queue-cli)
- [Cursor Semantics and Idempotency](#cursor-semantics-and-idempotency)
- [Live Transports](#live-transports)
- [Development](#development)
- [License](#license)

## Overview

MultiEdge Signal Relay distributes trading signals from publisher firms to their
institutional subscribers with a durable, sequenced log per strategy. This SDK gives you:

- **`SignalPublisher`** — idempotent publishing with automatic retries, and a disk
  dead-letter queue so a failed publish is never silently dropped.
- **`SignalSubscriber`** — at-least-once delivery with a persisted cursor: catch-up over
  REST, then live via polling or Azure Web PubSub, with gap detection and back-fill.
- **`verify_signature`** — HMAC-SHA256 webhook verification with constant-time comparison
  and replay protection.

The relay carries signals; it does not execute trades. Every delivery is sequenced and
auditable.

## Install

```bash
uv add multiedge-relay          # or: pip install multiedge-relay
uv add "multiedge-relay[webpubsub]"   # optional: Azure Web PubSub live transport
```

Requires Python 3.11+.

## Quickstart: Publish

```python
from multiedge_relay import Signal, SignalPublisher

with SignalPublisher(api_key="mek_your_api_key") as publisher:
    ack = publisher.publish(
        Signal(strategy_id="my-strategy", payload={"action": "BUY", "ticker": "SPY"})
    )
    print(ack.sequence, ack.signal_id)
```

## Quickstart: Subscribe

Offline for a weekend — you miss nothing: the subscriber resumes from its cursor,
replays everything you have not yet processed, then goes live.

```python
from multiedge_relay import ReceivedSignal, SignalMeta, SignalSubscriber

def on_signal(signal: ReceivedSignal, meta: SignalMeta) -> None:
    # MUST be idempotent — at-least-once delivery means replays can happen.
    print(f"[{meta.source}] seq={signal.sequence} {signal.payload}")

subscriber = SignalSubscriber(
    api_key="mek_your_api_key",
    strategy_id="my-strategy",
    on_signal=on_signal,
)
subscriber.run()   # blocks: catch-up from cursor, then live
```

To only drain the backlog (e.g. a cron job), use `subscriber.catch_up_only()`, which
returns the number of signals delivered.

## Webhook Verification

If you receive signals by webhook instead, verify every request before trusting it.
Verify the **raw received bytes** — never a re-serialized body.

```python
from multiedge_relay import SignatureVerificationError, verify_signature

def handle_webhook(raw_body: bytes, headers: dict[str, str]) -> None:
    try:
        signal = verify_signature(raw_body, headers, secret="whsec_your_endpoint_secret")
    except SignatureVerificationError as exc:
        raise PermissionError(f"rejected webhook: {exc}") from exc
    process(signal)
```

The signature is HMAC-SHA256 over `"{timestamp}." + raw_body`, sent as
`X-MultiEdge-Signature: sha256=<hex>` with `X-MultiEdge-Timestamp` (unix seconds).
Requests older than 5 minutes (configurable) are rejected to prevent replays.

## Error Handling — Never Silent Loss

A publish can end in exactly three ways — all of them explicit:

| Outcome | What you get |
|---|---|
| Accepted | `SignalAck` (with `deduplicated=True` if the relay had already seen this `client_signal_id`) |
| Rejected, not retryable | `AuthError` (401/403) or `ValidationRejected` (422/413) — raised immediately, never retried |
| Retries exhausted | Signal appended to the disk DLQ, then `PublishFailed` raised carrying `dlq_path` |

Retries: 5 attempts with exponential backoff (`0.5 · 2ⁿ` seconds, full jitter), only on
408/429/5xx and transport errors. Every signal gets an auto-generated ULID
`client_signal_id`, so retries and DLQ resends are idempotent on the relay side.

`publish_many(signals, raise_on_partial=False)` returns a list of
`SignalAck | PublishFailed` so batch jobs can account for every signal.

## Dead-Letter Queue CLI

Signals that exhausted retries live in `~/.multiedge/dlq/` as one JSONL file per
strategy per day. Recover them:

```bash
multiedge dlq list                       # show pending dead-lettered signals
multiedge dlq resend --dry-run           # what would be resent
multiedge dlq resend                     # resend; successes are removed from the DLQ
multiedge dlq purge                      # drop all pending entries (explicit data loss)
```

`resend` needs credentials: `--api-key` / `--base-url` or the environment variables
`MULTIEDGE_API_KEY` / `MULTIEDGE_BASE_URL`.

## Cursor Semantics and Idempotent-Callback Contract

- The subscriber persists the last **processed** sequence per strategy in
  `~/.multiedge/cursor/<strategy>.json` (atomic write: temp file + `os.replace`).
- The cursor is committed only **after** your `on_signal` callback returns. A crash
  mid-callback means that signal is redelivered on restart — **your callback must be
  idempotent** (e.g. key side effects on `signal.signal_id` or `signal.sequence`).
- Delivery is **at-least-once, in order**. Within a single run each sequence is
  delivered exactly once; across restarts you may see a signal again.
- A corrupt cursor file raises `CursorCorruptError` — it is **never silently reset**
  (a silent reset would replay the entire history into your callback). Inspect and fix
  with `multiedge cursor show` / `multiedge cursor reset --strategy X --to N`.

## Live Transports

- `live_transport="poll"` (default) — zero extra dependencies; repeats the catch-up
  query every `poll_interval` seconds. Ideal for minute-scale strategies.
- `live_transport="webpubsub"` — push delivery over Azure Web PubSub
  (`pip install "multiedge-relay[webpubsub]"`). The subscriber dedupes overlap by
  sequence, parks out-of-order messages, back-fills gaps over REST, and re-runs
  catch-up after any reconnect. Ordering truth is always the relay `sequence`, never
  the transport's own IDs. If a gap cannot be filled from REST, the subscriber raises
  `GapUnrecoverableError` instead of skipping data.

## Development

```bash
uv sync
uv run pytest -m "not integration"
uv run ruff check . && uv run black --check . && uv run mypy --strict src
```

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[Apache 2.0](LICENSE)
