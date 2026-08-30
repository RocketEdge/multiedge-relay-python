# multiedge-relay

Official Python SDK for MultiEdge Signal Relay — auditable signal-distribution infrastructure, not execution.

[![PyPI](https://img.shields.io/pypi/v/multiedge-relay.svg)](https://pypi.org/project/multiedge-relay/)
[![Python](https://img.shields.io/pypi/pyversions/multiedge-relay.svg)](https://pypi.org/project/multiedge-relay/)
[![CI](https://img.shields.io/badge/CI-passing-brightgreen)](https://github.com/RocketEdge/multiedge-relay-python/actions)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

## Table of Contents

- [Overview](#overview)
- [Install](#install)
- [Quickstart: Publish](#quickstart-publish)
- [Publishing a Whole Portfolio in One Signal](#publishing-a-whole-portfolio-in-one-signal)
- [Quickstart: Subscribe](#quickstart-subscribe)
- [Receiving a Whole Portfolio](#receiving-a-whole-portfolio)
- [Webhook Verification](#webhook-verification)
- [Sealed Mode — End-to-End Encryption](#sealed-mode--end-to-end-encryption)
- [Error Handling — Never Silent Loss](#error-handling--never-silent-loss)
- [Dead-Letter Queue CLI](#dead-letter-queue-cli)
- [Cursor Semantics and Idempotent-Callback Contract](#cursor-semantics-and-idempotent-callback-contract)
- [Exactly-Once Processing (SQLite)](#exactly-once-processing-sqlite)
- [Live Transports](#live-transports)
- [Two-Terminal Live Demo (Rebalance Feed)](#two-terminal-live-demo-rebalance-feed)
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
- **`SqliteStateStore`** — exactly-once *processing* on top of at-least-once delivery:
  one local SQLite file (stdlib, no extra dependency) that is both the subscriber's
  cursor store and a dedup ledger shared with webhook receivers.

The relay carries signals; it does not execute trades. Every delivery is sequenced and
auditable.

## Install

```bash
uv add multiedge-relay          # or: pip install multiedge-relay
uv add "multiedge-relay[webpubsub]"   # optional: Azure Web PubSub live transport
uv add "multiedge-relay[sealed]"      # optional: sealed mode (end-to-end encryption)
```

Requires Python 3.11+.

## Quickstart: Publish

```python
from multiedge_relay import Signal, SignalPublisher

with SignalPublisher(api_key="mesk_your_api_key") as publisher:
    ack = publisher.publish(
        Signal(
            strategy_id="my-strategy",
            # One signal carries the COMPLETE portfolio for one date.
            payload={
                "kind": "portfolio_rebalance",
                "signal_date": "2026-08-31",
                "planned_execution_date": "2026-09-01",
                "positions": [
                    {"ticker": "SPY", "action": "BUY", "signal_portfolio_weight": 0.6},
                    {"ticker": "TLT", "action": "SELL", "signal_portfolio_weight": 0.4},
                ],
            },
        )
    )
    print(ack.sequence, ack.signal_id)
```

That payload is the relay's shipped `portfolio_rebalance/1.0` schema, which a new feed
validates against by default — see [Publishing a Whole Portfolio in One
Signal](#publishing-a-whole-portfolio-in-one-signal) for why the whole book travels in one
signal, and what the limits are.

## Publishing a Whole Portfolio in One Signal

**One signal carries one complete portfolio state for one date.** That is why there is no
batch publish endpoint: the batching lives *inside* the payload, not across requests.

The relay ships a first-class schema for exactly this, `portfolio_rebalance/1.0`, and a new
feed validates against it by default:

```python
from multiedge_relay import Signal, SignalPublisher

positions = [
    {"ticker": "SPY", "action": "BUY", "signal_portfolio_weight": 0.60},
    {"ticker": "TLT", "action": "SELL", "signal_portfolio_weight": 0.40},
]

with SignalPublisher(api_key="mesk_your_api_key") as publisher:
    ack = publisher.publish(
        Signal(
            strategy_id="my-strategy",
            # Deterministic: re-running the feed re-acks instead of double-publishing.
            client_signal_id="my-strategy:2026-08-31",
            payload={
                "kind": "portfolio_rebalance",
                "signal_date": "2026-08-31",
                "planned_execution_date": "2026-09-01",
                "positions": positions,
            },
        )
    )
    print(ack.sequence, ack.duplicate)
```

The schema is `additionalProperties: false`, so field names are exact: it is
`signal_portfolio_weight`, not `target_weight`, and `action` is one of `INITIALIZE`, `BUY`,
`SELL`. Anything else is rejected with `422` as a `ValidationRejected`.

- **Size.** The payload cap is 64 KB (256 KiB sealed) — roughly 900 positions, so a real
  institutional book fits in one signal. Over the cap the relay answers `413`, which is
  **terminal**: `ValidationRejected` is never retried, so leave headroom.
- **Empty is meaningful.** `"positions": []` is a valid no-action heartbeat. Publish it —
  an absent day is indistinguishable from an outage, an empty one is not.
- **Not a portfolio?** Use whatever JSON your strategy's own registered schema allows; the
  relay carries the payload opaquely.

### Why not `publish_many`?

`publish_many([...])` is a convenience loop, not a batch API — it sends one HTTP request
per signal:

|                       | one signal, `positions[]`            | `publish_many([...])`                  |
| --------------------- | ------------------------------------ | -------------------------------------- |
| HTTP requests         | 1                                    | N                                      |
| Sequence numbers      | 1                                    | N                                      |
| Atomic                | yes                                  | **no** — a partial batch is possible   |
| Subscriber sees       | whole portfolio in one callback      | N callbacks, no completeness signal    |
| Use it for            | a portfolio / rebalance              | genuinely independent signals          |

Splitting one portfolio across N signals means a subscriber can crash mid-portfolio and
durably apply half of it, with nothing in the protocol marking where the portfolio ended.
Keep the portfolio in one signal and that failure mode does not exist.

## Quickstart: Subscribe

Offline for a weekend — you miss nothing: the subscriber resumes from its cursor,
replays everything you have not yet processed, then goes live.

```python
from multiedge_relay import ReceivedSignal, SignalMeta, SignalSubscriber

def on_signal(signal: ReceivedSignal, meta: SignalMeta) -> None:
    # MUST be idempotent — at-least-once delivery means replays can happen.
    print(f"[{meta.source}] seq={signal.sequence} {signal.payload}")

subscriber = SignalSubscriber(
    api_key="mesk_your_api_key",
    strategy_id="my-strategy",
    on_signal=on_signal,
)
subscriber.run()   # blocks: catch-up from cursor, then live
```

To only drain the backlog (e.g. a cron job), use `subscriber.catch_up_only()`, which
returns the number of signals delivered.

## Receiving a Whole Portfolio

Every transport delivers **one signal per message** — a webhook body is one signal, a live
frame is one signal, and REST catch-up pages are fanned out to your callback one at a time.
So you never receive several signals in one message. What you receive is one signal that
*contains* the whole portfolio:

```python
from multiedge_relay import ReceivedSignal, SignalMeta, SignalSubscriber

def on_signal(signal: ReceivedSignal, meta: SignalMeta) -> None:
    payload = signal.payload
    positions = payload["positions"]
    if not positions:
        return  # explicit no-action heartbeat, not a gap
    # The whole book arrived together — apply it as ONE unit.
    rebalance_to(
        {p["ticker"]: p["signal_portfolio_weight"] for p in positions},
        as_of=payload["signal_date"],
    )

SignalSubscriber(
    api_key="mesk_your_api_key", strategy_id="my-strategy", on_signal=on_signal
).run()
```

This is the safe shape, and the reason is the cursor: it is committed only **after**
`on_signal` returns, so one signal is one atomic apply-and-commit. Crash halfway and the
whole portfolio is redelivered (your callback must be idempotent). Split across N signals,
the cursor advances *between* legs and a crash leaves a durably half-applied portfolio that
the SDK cannot roll back — `SqliteStateStore.exactly_once_tx` is transactional per signal,
not per group.

`page_size` (default 500, server max 500) is transport paging for catch-up, not a
batch-receive API; changing it never changes what the callback sees.

### Collecting many signals as a group

To drain a backlog and work on it as a whole — a nightly reconciliation, a CSV export —
accumulate in the callback and use `catch_up_only()`, which returns the delivered **count**
(the signals come from your own list):

```python
received: list[ReceivedSignal] = []

subscriber = SignalSubscriber(
    api_key="mesk_your_api_key",
    strategy_id="my-strategy",
    on_signal=lambda signal, meta: received.append(signal),
    start_from="earliest",
)
count = subscriber.catch_up_only()   # returns how many were delivered
print(f"{count} signal(s); latest holds {len(received[-1].payload['positions'])} positions")
```

See [`examples/subscribe_rebalance_to_csv.py`](examples/subscribe_rebalance_to_csv.py) for
the full round trip, and [`examples/publish_rebalance_from_csv.py`](examples/publish_rebalance_from_csv.py)
for its publishing inverse.

Webhook receivers work the same way — `verify_signature` returns a single `ReceivedSignal`,
never a list.

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

## Sealed Mode — End-to-End Encryption

For strategies created with `sealed: true`, payloads are encrypted **before they
leave your process** and decrypted only inside your subscribers' processes. The
relay stores and forwards ciphertext and sees only envelope metadata — IDs,
sequence numbers, timestamps, size, recipient count. Not *"we promise not to
look"* but *"we cannot look"*.

**The cryptography (post-quantum hybrid, ahead of time):**

| Property | Mechanism |
|---|---|
| Confidentiality | Fresh 256-bit key per signal, ChaCha20-Poly1305 AEAD |
| Key wrap (per recipient) | Hybrid **X25519 + ML-KEM-768** (FIPS 203) via HKDF-SHA256 with transcript binding — secure if *either* component holds, so quantum-capable adversaries recording traffic today still cannot read it |
| Authenticity | Dual **Ed25519 + ML-DSA-65** (FIPS 204) publisher signatures; verifiers reject signature-stripping downgrades — the relay cannot forge signals |
| Identity binding | The AEAD binds `strategy_id` + `client_signal_id`, so an envelope replayed under another strategy fails decryption |

**Setup — subscriber (once):**

```bash
multiedge sealed keygen --kind recipient --out ~/.multiedge/keys/recipient.key.json
multiedge sealed register --key ~/.multiedge/keys/recipient.key.json --client CLIENT_ID --api-key mesk_...
# Read the printed fingerprint to your publisher over a separate channel.
```

**Setup — publisher (once):**

```bash
multiedge sealed keygen --kind sender --out ~/.multiedge/keys/sender.key.json
multiedge sealed register --key ~/.multiedge/keys/sender.key.json --strategy STRATEGY_ID --api-key mesk_...
```

**Publish sealed:**

```python
from multiedge_relay import Signal, SignalPublisher
from multiedge_relay.sealed import Sealer, SenderKeypair

sender = SenderKeypair.load("~/.multiedge/keys/sender.key.json")
sealer = Sealer.from_relay(
    api_key="mesk_...", strategy_id="STRATEGY_ID", sender=sender,
    # Strongest configuration: pin the fingerprints your subscribers read to
    # you over the phone / signed email — the relay then cannot substitute keys.
    pinned_recipients={"<64-hex fingerprint>", ...},
)
with SignalPublisher(api_key="mesk_...", sealer=sealer) as publisher:
    publisher.publish(Signal(strategy_id="STRATEGY_ID", payload={"weight": 0.5}))
```

**Subscribe sealed:**

```python
from multiedge_relay import SignalSubscriber
from multiedge_relay.sealed import RecipientKeypair, Unsealer

recipient = RecipientKeypair.load("~/.multiedge/keys/recipient.key.json")
unsealer = Unsealer.from_relay(
    api_key="mesk_...", strategy_id="STRATEGY_ID", recipient=recipient,
    pinned_sender="<the publisher's 64-hex fingerprint>",
)
subscriber = SignalSubscriber(
    api_key="mesk_...", strategy_id="STRATEGY_ID",
    on_signal=lambda s, m: print(s.payload),   # plaintext here — nowhere else
    unsealer=unsealer,
)
subscriber.run()
```

Webhook receivers pass the same `unsealer` to `verify_signature(...)` — HMAC
transport verification first, then unseal.

**Trust model, honestly:**

- The relay is untrusted even for key distribution: the SDK **recomputes every
  key fingerprint locally** and rejects mismatches; pinning fingerprints
  verified out-of-band (`display_fingerprint()` prints them in readable groups)
  removes the relay from the trust path entirely.
- The DLQ stores ciphertext (sealing happens before any spill), so a dead-letter
  file on disk leaks nothing.
- What sealed mode does **not** hide: envelope metadata (who publishes, how
  often, how large, to how many recipients) and traffic timing.
- Subscribers entitled *after* a signal was sealed cannot decrypt history —
  sealed envelopes are never re-encrypted (`NotARecipientError` says exactly
  this). Key rotation = register a new bundle, revoke the old; both stay
  decryptable by their holders.
- Capacity: up to ~100 entitled recipients per sealed strategy (256 KiB
  envelope cap). An increase is on the roadmap.
- Relay-side field redaction and the forbidden-term compliance scan require
  plaintext and are structurally unavailable on sealed strategies — the API
  rejects the combinations loudly.

## Error Handling — Never Silent Loss

A publish can end in exactly three ways — all of them explicit:

| Outcome | What you get |
|---|---|
| Accepted | `SignalAck` (with `duplicate=True` if the relay had already seen this `client_signal_id`) |
| Rejected, not retryable | `AuthError` (401/403) or `ValidationRejected` (422/413) — raised immediately, never retried |
| Retries exhausted | Signal appended to the disk DLQ, then `PublishFailed` raised carrying `dlq_path` |

Retries: exponential backoff (`0.5 · 2ⁿ` seconds, full jitter, capped at 8 s per sleep)
for up to **90 seconds of wall clock**, only on 408/429/5xx and transport errors. A
server `Retry-After` header overrides the computed backoff. Every signal gets an
auto-generated ULID `client_signal_id`, so retries and DLQ resends are idempotent on
the relay side.

The 90 s budget exists so that a **relay deployment is invisible to your publisher**: a
rolling revision swap answers 503 (or refuses connections) for tens of seconds, which is
longer than any attempt-counted budget survives. Tune it per publisher:

```python
# Latency-sensitive path: fail fast to the DLQ instead of blocking.
SignalPublisher(api_key=..., retry_budget_seconds=5.0)

# Batch job that must not dead-letter: ride out a longer maintenance window.
SignalPublisher(api_key=..., retry_budget_seconds=600.0)
```

`SignalSubscriber` is the mirror case: it is a long-running daemon with no DLQ to fall
back on, so its REST catch-up retries a transient failure **indefinitely** with the same
capped backoff, bounded by `stop()`. Every failed attempt is reported through `on_error`
— wire that up, because it is how an ongoing outage becomes visible. Pass
`max_attempts=` or `retry_budget_seconds=` to make catch-up fail fast instead.

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

## Exactly-Once Processing (SQLite)

If you would rather not write the idempotency yourself, `SqliteStateStore` does it for
you: one local SQLite file (default `~/.multiedge/state.db`, standard library only)
records which `signal_id`s your handler has **completed**, so a redelivery — crash
redelivery, reconnect overlap, webhook retry, operator replay — never re-invokes it.

```python
from multiedge_relay import SignalSubscriber, SqliteStateStore

store = SqliteStateStore()
subscriber = SignalSubscriber(
    api_key="mesk_your_api_key",
    strategy_id="my-strategy",
    on_signal=store.exactly_once(handle),   # handle completes at most once per signal
    cursor_store=store,                     # same file doubles as the cursor store
)
subscriber.run()
```

The marker is committed **atomically with your handler's success**: a handler exception
rolls it back, so the signal is retried on the next delivery. Webhook receivers use the
same store (dedup works across channels because the key is the globally unique
`signal_id`):

```python
with store.process(signal) as fresh:
    if fresh:
        ...  # side effects here — runs at most once per signal
# answer 2xx either way so the relay's retry ladder stops
```

What "exactly once" honestly means here:

- Handler runs and **local state** are exactly-once: with `store.exactly_once_tx` your
  handler receives a `sqlite3.Cursor` bound to the marker's transaction — rows you write
  through it commit and roll back together with the marker.
- **External** side effects (order placement, e-mail) keep a tiny at-least-once window:
  a crash after your handler returns but before the marker commits re-runs the handler
  once on redelivery. No client-side scheme can close that window for effects outside
  the database.
- The store prunes itself (markers below the cursor watermark are deleted on every
  cursor commit; older-than-90-days markers — the relay's replay window — are pruned on
  open) and vacuums freed pages, so the file stays small.
- A corrupt state file raises `StateStoreCorruptError` and is **never silently reset**
  (that would forget what was processed and replay history into your handler).
- Single process per state file; within the process it is thread-safe.

## Live Transports

- `live_transport="poll"` (default) — zero extra dependencies; repeats the catch-up
  query every `poll_interval` seconds. Ideal for minute-scale strategies.
- `live_transport="webpubsub"` — push delivery over Azure Web PubSub
  (`pip install "multiedge-relay[webpubsub]"`). The subscriber dedupes overlap by
  sequence, parks out-of-order messages, back-fills gaps over REST, and re-runs
  catch-up after any reconnect. Ordering truth is always the relay `sequence`, never
  the transport's own IDs. If a gap cannot be filled from REST, the subscriber raises
  `GapUnrecoverableError` instead of skipping data.

## Two-Terminal Live Demo (Rebalance Feed)

An end-to-end, watch-it-happen test of the relay using two terminals on one
machine: Terminal 1 runs a consumer, Terminal 2 a producer replaying a synthetic
monthly-rebalance instruction feed — one signal per market day, `PORTFOLIO/NONE`
days included as explicit empty heartbeats. The scripts live in
[`examples/prod_demo/`](examples/prod_demo/):

| Script | Role |
| --- | --- |
| `generate_demo_csv.py` | Deterministic synthetic instruction CSV (no real data) |
| `setup_demo.py` | One-time control-plane bootstrap: strategy, client, endpoint, entitlement, keys |
| `producer_rebalance.py` | Terminal 2 — paced publisher with idempotent `client_signal_id`s |
| `consumer_rebalance.py` | Terminal 1 — polling subscriber with a persisted cursor |

The commands below are PowerShell (Windows); on macOS/Linux replace
`$env:NAME = "..."` with `export NAME=...`.

### 1. Install

```powershell
git clone https://github.com/RocketEdge/multiedge-relay-python
cd multiedge-relay-python\examples\prod_demo
```

That is the whole install step. Every command below runs through
[`uv`](https://docs.astral.sh/uv/): `uv run` finds the project root from any
subdirectory, creates and syncs the environment from `uv.lock` on first use, and runs
the script inside it — so there is no virtual environment to build or activate, in
either terminal.

Without `uv`, build the environment once (`python -m venv .venv`, activate it,
`pip install multiedge-relay`) and drop the `uv run` prefix from every command. A bare
`python demo.py` in an unactivated shell runs the interpreter on `PATH`, where the SDK
is absent — the scripts stop with an explicit message naming that interpreter.

### 2. Generate the demo instruction feed

```powershell
uv run python generate_demo_csv.py --seed 42 --out demo_rebalance_signals.csv
```

The file has the shape of a real model-portfolio feed —
`SignalDate,PlannedExecutionDate,Ticker,Action,SignalPortfolioWeight,TradeWeightDelta,ImpliedPostTradeWeightAtSignalClose`
— with an `INITIALIZE` day, month-end rebalances, an ad-hoc mid-month rebalance,
daily `PORTFOLIO/NONE` heartbeat rows, and one asset joining the allocation
mid-history. Same seed, same bytes, anywhere.

### 3. Bootstrap the strategy and keys (once)

You need a tenant **admin** API key (`mesk_...`) from your MultiEdge operator.
Use a dedicated evaluation/test tenant — the demo writes real signals into that
tenant's ledger.

```powershell
$env:MULTIEDGE_ADMIN_KEY = "mesk_your_tenant_admin_key"
uv run python setup_demo.py
```

This creates strategy `demo-rebalance` (pinned to the relay's default
`portfolio_rebalance/1.0` schema), a demo subscriber client with a `rest_pull`
endpoint and an active entitlement, and mints two keys. **Copy all three values
now — the keys are shown exactly once:**

```text
strategy_id   = 01J...ULID   (slug: demo-rebalance)
PUBLISHER  key (Terminal 2 producer): mesk_...
SUBSCRIBER key (Terminal 1 consumer): mesk_...
```

Reruns are safe: the strategy and client are reused; keys are minted fresh
(revoke stale ones in the portal).

### 4. Terminal 1 — start the consumer

```powershell
$env:MULTIEDGE_API_KEY = "mesk_the_SUBSCRIBER_key"
uv run python consumer_rebalance.py --strategy-id 01J...ULID
```

It catches up on anything already published, then polls live every 2 s. Leave it
running.

### 5. Terminal 2 — start the producer

```powershell
$env:MULTIEDGE_API_KEY = "mesk_the_PUBLISHER_key"
uv run python producer_rebalance.py demo_rebalance_signals.csv --strategy-id 01J...ULID --pace 3 --limit 40
```

One signal date is published every 3 seconds (`--limit 40` keeps the first demo
short; drop it for a full-history soak). Terminal 1 prints each arrival within a
poll interval:

```text
[live] seq=12 2024-01-16 -> 2024-01-17: no action (heartbeat)
[live] seq=13 2024-01-31 -> 2024-02-01: 8 position(s) — 4 BUY, 4 SELL
```

### 6. Prove the delivery guarantees

- **Crash resume (the headline):** press Ctrl+C in Terminal 1 while the producer
  is still publishing. Wait a few producer ticks, restart the same consumer
  command — it resumes from its persisted cursor and delivers exactly the missed
  signals as `[catchup]`, no loss, no duplicates.
- **Idempotent republish:** re-run the producer command unchanged. Every ack
  reports `(deduplicated)` — the deterministic
  `client_signal_id = "<strategy_id>:<signal_date>"` means the relay returns the
  original acks instead of storing copies.
- **Reconstruction:** `uv run python consumer_rebalance.py --strategy-id 01J...ULID
  --catchup-only --out received.csv` rebuilds the instruction rows from the
  relay's log (the schema carries ticker, action, and pre-trade weight; the two
  derived delta columns are not transported).

Demo state (cursor, DLQ) lives under `examples/prod_demo/.demo/`, not in
`~/.multiedge/`. To clean up afterwards, revoke the two demo keys in the portal;
deleting `.demo/` resets the consumer to a first run.

## Development

```bash
uv sync
uv run pytest -m "not integration"
uv run ruff check . && uv run black --check . && uv run mypy --strict src
```

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[Apache 2.0](LICENSE)
