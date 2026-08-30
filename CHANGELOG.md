# Changelog

All notable changes to `multiedge-relay` are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.7.1] - 2026-08-31

No library changes — this release exists to prove the publishing pipeline.

### Changed

- **Releases are now published by CI via PyPI trusted publishing.** Every prior
  version reached PyPI by hand: the `Release` workflow had been correct since
  `v0.5.0`, but no trusted publisher was ever registered on PyPI, so all four runs
  died at `invalid-publisher` and the artifacts were uploaded from a laptop
  instead. Registering the publisher fixed the exchange; the workflow now also
  pins its actions (the OIDC-holding step no longer tracks a branch pointer),
  refuses a tag that disagrees with the packaged version, and deploys from a
  `pypi` environment restricted to `v*` tags. The release procedure is documented
  in `CONTRIBUTING.md` — no API token exists, and manual uploads are forbidden.

## [0.7.0] - 2026-08-30

### Fixed

- **`live_transport="webpubsub"` now matches the deployed relay.** It could never
  have worked before — three drifts at once, each invisible because the fake relay
  in the test suite accepted the SDK's own dialect (the same failure mode as the
  0.6.0 `duplicate` rename, one layer down):
  1. **Negotiate sent the wrong body.** The SDK posted `{"strategy_id": ...}`;
     the relay's `/v1/ws/negotiate` requires `{"endpoint_id": ...}` (tokens are
     minted per ENDPOINT group) and answers 422 to anything else — every connect
     attempt failed and retried forever.
  2. **Frames were parsed as the wrong shape.** The relay pushes
     `{"envelope": "<raw envelope JSON string>", "signature": "<hex>",
     "timestamp": <unix_s>}`; the SDK tried to parse the whole frame as a
     `ReceivedSignal`, which can only raise `ValidationError`.
  3. **The frame signature was never verified.** The per-endpoint HMAC (the
     reason the envelope travels as a raw string at all) was ignored.
  The fake relay's negotiate endpoint now mirrors the real server (records the
  body, 422s a missing `endpoint_id`), so this class of drift is caught.

### Added

- `SignalSubscriber(..., endpoint_id=..., endpoint_secret=...)` — both REQUIRED
  for `live_transport="webpubsub"` (a `ValueError` at construction otherwise):
  `endpoint_id` is the subscriber's websocket endpoint ULID; `endpoint_secret`
  is the endpoint's signing secret (`secret_base64`, shown once at creation).
- **Every push frame is HMAC-verified before parsing** (HMAC-SHA256 over
  `"{timestamp}." + envelope-utf8-bytes`, same key precedence as webhooks,
  ±5 min freshness). A frame that fails verification is reported through
  `on_error` and dropped — a forged or replayed frame can neither be delivered
  nor halt the subscriber; any genuine gap it conceals is recovered from REST
  by sequence arithmetic.
- `verify_ws_frame(frame, secret)` — the frame-verification primitive, exported
  for hand-rolled websocket consumers (the ws twin of `verify_signature`).
- **Pre-live buffering per the ws-resume protocol:** frames arriving between
  socket-open and the end of REST catch-up are buffered (verified, unprocessed)
  and drained in arrival order afterwards, deduped by sequence — the spec's
  connect-buffer-catchup-drain ordering instead of racing the two feeds.
- Negotiate configuration errors (4xx other than 429: unknown endpoint,
  endpoint not websocket, endpoint not active) now raise `ValidationRejected`
  out of `run()` instead of retrying an identical doomed request forever.
- `ws_client_factory` constructor seam so the transport is testable without the
  `[webpubsub]` extra installed.

## [0.6.1] - 2026-08-30

### Fixed

- **The README quickstart still published a single-ticker payload**, directly above the
  new section explaining that the field is `signal_portfolio_weight` and not
  `target_weight`. PyPI renders the README as the project page, so the first code a
  reader saw contradicted the page it sat on — and would be refused with 422 by any feed
  on the default `portfolio_rebalance/1.0` schema. The quickstart now publishes a whole
  portfolio and links to the section.
- `examples/seal_publish.py` carried the same `target_weight` payload;
  `examples/publish_batch_with_dlq.py` sliced one portfolio into one signal per ticker —
  the exact anti-pattern `publish_many` should not demonstrate. It now backfills three
  separate days, each a complete portfolio, which is what genuinely independent signals
  look like.
- `tests/test_docs_contract.py` pinned only one example's payload, which is how three
  other samples drifted past it. It now scans every shipped file for fields the standard
  schema rejects.

## [0.6.0] - 2026-08-30

### Changed

- **BREAKING: `SignalAck.deduplicated` is now `SignalAck.duplicate`.** The relay sends
  `duplicate`; the model declared a name the wire never carries, and pydantic ignores
  unknown fields — so the flag was never actually parsed. It was correct only because
  the publisher force-set it whenever the response status was 200. The fake relay in
  the test suite emitted the SDK's name too, so every test agreed with itself and the
  drift was unobservable; the fake now speaks the relay's dialect, and a new test
  serves `duplicate` on a **201** so the body is isolated as the source of truth.
  Migrate with `ack.deduplicated` → `ack.duplicate`; the old name still works as a
  deprecated property and is removed in 1.0.

### Added

- **`ReceivedSignal.correlation_id`** — the publisher's tracing id, echoed by the relay
  on every read path (catch-up, webhook, live). It was being dropped on the floor:
  entitlement field policies filter only `payload`, so it was always available and
  never surfaced. Additive; the sealed envelope's AAD binds
  `strategy_id`/`client_signal_id`/`sender_kid` only, so the frozen v1 wire vector is
  untouched.
- **README § "Publishing a Whole Portfolio in One Signal"** and **§ "Receiving a Whole
  Portfolio"**, plus `publish` / `publish_many` / `on_signal` / `page_size` docstrings.
  The quickstart taught a single-ticker payload, so the only documented way to send a
  book was to guess — a loop, or a batch API that does not exist. One signal carries
  one complete portfolio state (`portfolio_rebalance/1.0`, ~900 positions in the 64 KB
  cap); `publish_many` is N requests, N sequences and **not atomic**, and splitting a
  portfolio across signals lets a subscriber durably apply half of one.

### Fixed

- **Every quickstart, example and docstring used an invalid `mek_` API-key
  placeholder.** The relay accepts only `mesk_` keys, so a copy-pasted sample was
  rejected at auth before the reader reached anything the SDK does. `publish_minimal.py`
  also published a single-ticker payload that a feed pinned to the default
  `portfolio_rebalance/1.0` schema refuses with 422 (`additionalProperties: false`).
  Both are now guarded by `tests/test_docs_contract.py`, which reads the shipped text —
  prose defects are invisible to every other test in the suite.

## [0.5.0] - 2026-08-30

### Fixed

- **The exactly-once ledger no longer balloons on SQLite 3.51+.** `prune()` and
  `commit()` drove `PRAGMA incremental_vacuum` with a single
  `execute(...).fetchall()`. The pragma reclaims one page per step, and through
  SQLite 3.50 it emitted a row per reclaimed page, so `fetchall()` incidentally
  stepped it to exhaustion. SQLite 3.51+ returns no rows, `fetchall()` stops
  immediately, and each call freed exactly one page — so a long-running
  subscriber's `state.db` grew without bound, the very thing
  `auto_vacuum=INCREMENTAL` was chosen to prevent. An explicit page count does not
  help (`incremental_vacuum(N)` still yields after one page). Both call sites now
  share a helper that drains the freelist and stops on the first iteration that
  frees nothing. Surfaced by CI on Python 3.11, which bundles SQLite 3.53.1 while
  3.12-3.14 bundle 3.50.4.

- **The two-terminal demo no longer dies with a bare `ModuleNotFoundError`.** The
  README's install step created a virtual environment but never activated it, so a
  copy-pasted `python consumer_rebalance.py` ran under whatever interpreter was on
  `PATH` — where the SDK is not installed. Every demo command is now prefixed with
  `uv run` (README and script usage docstrings), which builds and uses the right
  environment itself and leaves nothing to activate per terminal; the install step is
  just the clone. `consumer_rebalance.py`, `producer_rebalance.py`, and
  `setup_demo.py` replace the import traceback with a message naming the missing
  module, the interpreter that could not find it, and that `uv run` command.

### Changed

- **Retries now ride out a relay deployment.** The retry budget is wall-clock, not
  attempt-counted: `SignalPublisher` / `AsyncSignalPublisher` keep retrying a
  transient failure for **90 seconds** by default (new `retry_budget_seconds=`
  parameter) instead of giving up after 5 attempts (~7.5 s worst case). A rolling
  Azure Container Apps revision swap or a migration bundle answers 503 — or refuses
  connections — for tens of seconds, so the old budget dead-lettered every in-flight
  publish through a routine deployment. Classification is unchanged (408/429/5xx and
  transport errors only), as are the DLQ spill and `PublishFailed` on exhaustion.
- **`SignalSubscriber` no longer dies on a deployment.** REST catch-up retries a
  transient failure indefinitely by default (`max_attempts` and
  `retry_budget_seconds` both default to `None`), bounded by `stop()` — a daemon with
  no DLQ must ride out an outage rather than raise `MultiEdgeError` out of `run()`
  and require an operator restart. Every failed attempt is now reported through
  `on_error`, so an unbounded loop is never a silent one. Terminal statuses (401/403,
  and any non-retryable code) still raise immediately.
- Backoff is capped at **8 seconds per sleep** (`MAX_DELAY_SECONDS`) so a long budget
  cannot produce one enormous unresponsive wait; the subscriber slices its sleeps so
  `stop()` is honoured within a second.
- `DEFAULT_MAX_ATTEMPTS` raised 5 → 25. It is now a safety net; the wall-clock budget
  is the bound that binds. `max_attempts=` still caps the loop when passed explicitly,
  and `max_attempts` / `retry_budget_seconds` accept `None` for "no cap".

### Added

- `Retry-After` is now honoured on 429 and 503 (both RFC 9110 forms: delta-seconds and
  HTTP-date), clamped to 300 s and to the remaining budget. The server's hint wins over
  the computed backoff; an unparseable header falls back to exponential backoff.
- `multiedge_relay._retry.RetryPolicy`: the single retry policy consulted by the sync
  publisher, the async publisher, and the subscriber — previously three copies of the
  same loop that could drift.
- `monotonic=` test seam on all three clients, so a fake clock can spend a 90 s budget
  instantly and deterministically.
- `examples/prod_demo/`: a two-terminal live demo of the relay — deterministic
  synthetic rebalance-feed generator, one-shot control-plane bootstrap script,
  paced idempotent producer, and a cursor-resuming polling consumer — with a full
  step-by-step guide in README § "Two-Terminal Live Demo (Rebalance Feed)".

## [0.4.0] - 2026-08-17

### Added

- **Sealed mode — end-to-end encryption** (`pip install "multiedge-relay[sealed]"`,
  `cryptography>=47`): publishers seal payloads client-side and subscribers unseal
  them client-side, so the relay stores and forwards only ciphertext — *"not 'we
  promise not to look' but 'we cannot look'"*. The scheme is post-quantum hybrid,
  ahead of the 2030/2035 classical-crypto deprecation timelines: per-signal
  ChaCha20-Poly1305 with a fresh 256-bit key, wrapped per recipient with
  **X25519 + ML-KEM-768** (FIPS 203) combined through HKDF-SHA256 with full
  transcript binding, and signed with dual **Ed25519 + ML-DSA-65** (FIPS 204)
  signatures — verifiers reject signature-stripping downgrades. The AAD binds
  `strategy_id` + `client_signal_id`, so an envelope replayed under another
  strategy or publish identity fails authenticated decryption.
- `multiedge_relay.sealed`: `RecipientKeypair` / `SenderKeypair` (JSON key files,
  created `0600`, never overwritten), `seal` / `unseal`, `Sealer` / `Unsealer`
  with `from_relay(...)` constructors that fetch key bundles and **recompute
  every fingerprint locally** — the relay is untrusted for key authenticity —
  plus optional pinning (`pinned_recipients` / `pinned_sender`) against
  out-of-band-verified fingerprints.
- `SignalPublisher(..., sealer=)` / `AsyncSignalPublisher(..., sealer=)`: sealing
  happens after the idempotency ULID is assigned and before any DLQ spill, so
  dead-letter files hold ciphertext and resends are byte-identical.
  `SignalSubscriber(..., unsealer=)` and `verify_signature(..., unsealer=)`
  decrypt before your callback; an unseal failure routes to `on_error` and the
  cursor is NOT committed — never silent loss, never unverified ciphertext.
- CLI: `multiedge sealed keygen|fingerprint|register` (lazy import; the rest of
  the CLI works without the extra).
- `ReceivedSignal.client_signal_id` (optional, additive): the relay envelope's
  idempotency echo, required to reconstruct the sealed AAD.
- New exceptions in core (stdlib-only): `SealedError`, `UnsealError`,
  `NotARecipientError`, `SealedKeyError`, `KeyPinningError`.

### Changed

- Development toolchain pinned to Python 3.14 (`.python-version`); CI matrix adds
  3.14 plus a core-only job that removes `cryptography` and proves the sealed
  extra never became a hard dependency. Supported floor stays Python 3.11.

Honest limits: sealed mode supports up to ~100 entitled recipients per strategy
(256 KiB envelope cap; an increase is on the roadmap); subscribers entitled after
a signal was sealed cannot decrypt history (`NotARecipientError` says exactly
this); relay-side field redaction and the forbidden-term compliance scan are
structurally unavailable on sealed strategies; envelope metadata (IDs, sequence,
timestamps, size, recipient count) remains visible to the relay by design.

## [0.3.0] - 2026-08-17

### Added

- `SqliteStateStore`: exactly-once *processing* on top of the relay's at-least-once
  delivery. One local SQLite file (stdlib `sqlite3`, no new dependency) is both a
  drop-in `CursorStore` for `SignalSubscriber` and a processed-signal ledger keyed
  on the globally unique `signal_id`. `store.exactly_once(handler)` wraps the
  subscriber callback (marker committed atomically with handler success; duplicates
  skipped, cursor still advances); `store.exactly_once_tx(handler)` additionally
  hands the handler a cursor bound to the marker transaction for truly exactly-once
  local state; `store.process(signal)` / `store.seen(signal_id)` give webhook
  receivers the same dedup across the retry ladder and operator replays.
  Self-pruning (watermark prune on cursor commit, 90-day age prune matching the
  relay's replay window) with incremental vacuum, WAL + `synchronous=FULL`
  durability, thread-safe within one process.
- `StateStoreCorruptError` (subclass of `CursorCorruptError`): a corrupt or
  unknown-version state file raises loudly and is never silently reset.
- Example `subscribe_exactly_once.py`; the FastAPI and Flask webhook examples now
  show real dedup via `store.process(...)` instead of a placeholder comment.

### Fixed

- `__version__` (previously `0.1.1`) re-synced with the package version from
  `pyproject.toml` (previously `0.2.0`) — the User-Agent header now reports the
  true version.

## [0.2.0] - 2026-08-17

### Changed

- Default API endpoint is now `https://relay-api.multiedge.ai` (was
  `https://api.multiedge.ai`, which was never provisioned). MultiEdge products
  each live at `<product>-api.multiedge.ai`; `api.multiedge.ai` is reserved for
  a future shared gateway. Explicit `base_url=` arguments are unaffected.

## [0.1.1] - 2026-08-16

### Fixed

- `verify_signature` keyed HMAC-SHA256 with the endpoint secret string's UTF-8
  bytes, but the relay signs with the base64-DECODED 32 raw bytes (endpoint
  secrets are delivered as base64 of 32 random bytes) — genuine deliveries failed
  verification. The key is now the decoded bytes when the secret is valid
  base64-of-32-bytes, with UTF-8 fallback for ad-hoc secrets (documented
  precedence; regression-tested both ways).
- `FileCursorStore.commit` could crash with `PermissionError` on Windows when a
  concurrent reader briefly held the cursor file open during `os.replace`
  (sharing violation). The replace is now retried up to 5 times with a 20 ms
  backoff (`PermissionError` only); other `OSError`s and exhausted retries still
  raise.

## [0.1.0] - 2026-08-16

### Added

- `SignalPublisher` / `AsyncSignalPublisher`: idempotent publishing with auto-ULID
  `client_signal_id`, exponential backoff with full jitter (5 attempts, retry only on
  408/429/5xx/transport errors), typed terminal errors (`AuthError`,
  `ValidationRejected`), and disk DLQ spill on retry exhaustion (`PublishFailed`
  carries the `dlq_path`).
- `DiskDLQ`: one JSONL file per strategy per day under `~/.multiedge/dlq`;
  `pending()`, `resend()` (successes removed, failures kept without duplication),
  `purge()`.
- `SignalSubscriber`: at-least-once delivery with a durable per-strategy cursor
  (`~/.multiedge/cursor`, atomic writes, corrupt files raise — never silently
  reset); REST catch-up then live via polling or Azure Web PubSub
  (`[webpubsub]` extra); overlap dedupe by sequence; gap parking + REST gap-fill;
  `GapUnrecoverableError` instead of silent skips; `catch_up_only()` for batch use.
- `verify_signature`: webhook HMAC-SHA256 verification (constant-time compare,
  5-minute freshness window, injectable clock).
- `multiedge` CLI: `dlq list|resend [--dry-run]|purge`, `cursor show|reset`.
- Examples: minimal publish, batch publish with DLQ, catch-up subscriber, FastAPI
  and Flask webhook receivers, portfolio-rebalance CSV publisher/reconstructor.
- Typed package (`py.typed`), Python 3.11–3.13.
