# CLAUDE.md — multiedge-relay (Python SDK)

Official Python SDK for MultiEdge Signal Relay — auditable signal-distribution
infrastructure, not execution. Public repo, Apache 2.0, published to PyPI as
`multiedge-relay` (module `multiedge_relay`). Users are quant developers at publisher
firms and their institutional subscriber clients.

This file and `.github/copilot-instructions.md` are the SAME contract for two agents;
keep them in sync in the same commit.

## THE COMMANDS

```powershell
uv sync                                   # install (Python 3.14 dev default; supports 3.11–3.14)
uv run pytest -m "not integration"        # unit tests (no network)
uv run pytest -m integration              # live tests (needs MULTIEDGE_INTEGRATION_URL + key)
uv run ruff check .                       # lint
uv run black --check .                    # format check
uv run mypy --strict src                  # strict type check
uv build                                  # build sdist+wheel (hatchling)
```

All gates (pytest non-integration, ruff, black --check, mypy --strict) must be green
before claiming any task complete. Run them fresh.

## Design contract (mirrors the relay backend — do not drift)

1. **Never silent loss.** Publish failures surface explicitly: `AuthError` /
   `ValidationRejected` (no retry), `PublishFailed` once the retry budget is spent,
   carrying `dlq_path` when spilled to the disk DLQ (`~/.multiedge/dlq/*.jsonl`).
   `multiedge dlq list|resend` CLI recovers. No bare `except`, no swallowed errors.
2. **At-least-once subscriber with cursor.** `SignalSubscriber` persists the last
   processed `sequence` per strategy in `~/.multiedge/cursor/<strategy>.json`
   (atomic write: tmp + `os.replace`), committed only AFTER `on_signal` returns —
   crash ⇒ redelivery; the callback contract is documented idempotent.
   A corrupt cursor file raises loudly; it never silently resets.
3. **Catch-up algorithm** (the product's core promise): on start, REST catch-up from
   cursor (`GET /v1/signals?strategy_id&since_sequence&limit`, `next_sequence` pages),
   then live (Web PubSub via the optional `[webpubsub]` extra, or `live_transport="poll"`
   with zero Azure deps); overlap deduped by sequence; a live gap parks messages in a
   buffer, REST-fills the range, then drains in order. Ordering truth is the relay
   `sequence` — never the transport's own sequence IDs.
4. **Idempotent publish.** `client_signal_id` auto-ULID per signal; the relay returns
   the original ack for duplicates (`SignalAck.duplicate=True`) — retries are safe.
   **Model field names mirror the relay's OWN wire names, and `tests/fake_relay.py`
   must emit those names.** `SignalAck.duplicate` was `deduplicated` — a key the relay
   never sends, so pydantic dropped it and only the 200-status fallback kept it
   correct; the fake emitted the SDK's spelling too, so the suite could not see the
   drift. A fake that speaks the client's dialect only ever confirms the client's
   assumptions.
4a. **One signal = one COMPLETE portfolio state.** `payload` (≤64 KB, 256 KiB sealed;
   413 is terminal) carries the whole book — the shipped `portfolio_rebalance/1.0`
   schema is an unbounded `positions` list for one signal date, ~900 positions in the
   cap. There is NO batch publish endpoint: `POST /v1/signals` binds one signal, and
   `publish_many` is a client-side loop — N requests, N sequences, **not atomic**.
   Never document it as a way to send a portfolio: the cursor commits per signal, so a
   split portfolio can be durably half-applied with no completeness marker. Receiving
   mirrors this — every transport delivers one signal per message and `page_size` is
   transport paging, never a batch-receive API.
5. **Webhook verification**: HMAC-SHA256 over `"{timestamp}." + raw_body` with the
   endpoint secret, `hmac.compare_digest`, reject |now − ts| > 5 min, injectable clock.
   Verify raw received bytes — never re-serialize.
6. **TDD**; runtime deps only `httpx` + `pydantic` (extras: `[webpubsub]`, `[sealed]`);
   mypy --strict; Google docstrings on every public class/function; py.typed shipped;
   frozen pydantic v2 models; SemVer; CHANGELOG kept current.
7. **Public repo hygiene:** no business plans, internal strategy, credentials, or
   Azure resource details in this repo. Examples use placeholder keys and
   `https://relay-api.multiedge.ai`. Placeholder API keys are **`mesk_`** — the only
   prefix the relay accepts; `tests/test_docs_contract.py` fails the build on any
   other, because a wrong placeholder makes every copy-pasted quickstart 401.
8. **Latest supported versions.** Dependencies and the Python matrix ride the newest
   released versions the toolchain supports; verify ceilings, never guess, and record
   any forced pin with its reason here.
9. **Exactly-once processing store (`state_sqlite.py`).** `SqliteStateStore` is one
   SQLite file (stdlib, WAL + `synchronous=FULL`) that is BOTH a `CursorStore` and a
   processed-`signal_id` ledger. Invariant: a signal is processed iff
   `sequence <= cursor[strategy]` OR `signal_id ∈ processed`; the handler only runs
   inside the transaction that records the marker on success, so it never completes
   twice — subscriber wrapper (`exactly_once`/`exactly_once_tx`) and webhook helper
   (`process`/`seen`) share the ledger. Duplicate deliveries return normally (the
   subscriber must still advance its cursor). Markers at/below the cursor watermark
   are pruned on every commit; age-prune default 90 d = the relay replay window
   (older deliveries cannot recur); prunes end in `incremental_vacuum` so the file
   never balloons. A corrupt/unknown-version file raises `StateStoreCorruptError`
   (subclass of `CursorCorruptError`, hence subscriber-fatal) — never silent reset.
   External side effects keep a tiny at-least-once window (crash between handler
   return and marker COMMIT) — documented honestly, never claim exactly-once
   *delivery*.
10. **Sealed mode (relay ADR 0004): core never imports `sealed`.** The crypto lives
    exclusively in `multiedge_relay/sealed/` behind the `[sealed]` extra
    (`cryptography>=47`, which ships ML-KEM-768/ML-DSA-65); the core package stays
    importable and fully functional without it (CI has a core-only job proving it),
    and `Sealer`/`Unsealer` reach core only via `TYPE_CHECKING` + duck-typed params.
    The sealed envelope v1 wire format (AAD = domain‖strategy_id‖client_signal_id‖
    sender_kid; hybrid X25519+ML-KEM-768 → HKDF-SHA256 with transcript; dual
    Ed25519+ML-DSA-65 with strip-downgrade rejection; canonical JSON everywhere) is
    FROZEN by the committed test vector `tests/fixtures/sealed_v1_vector.json` —
    format changes require a new `sealed: "v2"` and a new vector, never an edit.
    Sealing happens inside `prepare_signal` AFTER ULID assignment and BEFORE DLQ
    spill (ciphertext at rest, byte-identical resends). Key bundles fetched from the
    relay are ALWAYS re-fingerprinted locally; the relay is untrusted for key
    authenticity — pinning against out-of-band fingerprints is the documented
    strongest configuration. Never log, print, or serialize private key material.
11. **Retry rides out a relay deployment, and the budget is WALL CLOCK.** One
    `RetryPolicy` (`_retry.py`) serves all three call sites — sync publisher, async
    publisher, subscriber catch-up — because three hand-rolled copies of the loop
    drift. Publishers retry a transient failure (408/429/5xx + `httpx.TransportError`)
    for `retry_budget_seconds=90` by default; `max_attempts` (25) is only a safety
    net. The bound is wall clock because the failure being ridden out — an Azure
    Container Apps revision swap or a migration bundle — has a DURATION, not an
    attempt count: the previous 5-attempt budget expired in ~7.5 s and dead-lettered
    every in-flight publish through a routine deploy. Backoff is capped at 8 s per
    sleep so a long budget never becomes one unresponsive wait, and a server
    `Retry-After` overrides the computed backoff (clamped to 300 s and to the
    remaining budget). `SignalSubscriber` is the mirror case: a daemon with no DLQ,
    so its catch-up retries INDEFINITELY by default, bounded by `stop()` (sleeps are
    sliced so stop lands within a second) and reported through `on_error` on every
    attempt — an unbounded loop must never be a silent one. Terminal statuses stay
    terminal: a 400 is never retried, budget or no budget.

## Layout

```text
src/multiedge_relay/   models, ulid, exceptions, _retry, _http,
                       publisher, publisher_async, dlq, cursor, subscriber,
                       state_sqlite (exactly-once processing store),
                       webhook, cli (Web PubSub live transport is inlined in
                       subscriber.py behind the [webpubsub] extra)
src/multiedge_relay/sealed/   [sealed] extra ONLY — keys.py (hybrid keypairs +
                       fingerprints), core.py (seal/unseal, the whole crypto),
                       registry.py (Sealer/Unsealer + from_relay + register helpers)
tests/                 fake_relay.py (in-proc FastAPI via httpx.ASGITransport, incl.
                       sealed-key routes) + suites; fixtures/sealed_v1_vector.json
                       freezes the sealed wire format; markers: integration
examples/              publish_minimal, publish_batch_with_dlq, subscribe_catchup,
                       subscribe_exactly_once, webhook_fastapi, webhook_flask,
                       publish_rebalance_from_csv, seal_publish, unseal_subscribe
examples/prod_demo/    two-terminal live demo (README § "Two-Terminal Live Demo"):
                       generate_demo_csv (deterministic SYNTHETIC rebalance feed —
                       never real client data in this public repo), setup_demo
                       (control-plane bootstrap), producer_rebalance (paced,
                       idempotent client_signal_id "<strategy_id>:<signal_date>"),
                       consumer_rebalance (poll subscriber; state under
                       examples/prod_demo/.demo/, never ~/.multiedge/).
                       The demo is documented and run as `uv run python <script>` (no
                       venv to activate, in either terminal), and every third-party
                       import here is guarded: a missing dep exits with the module,
                       the interpreter (sys.executable) and that same uv command — an
                       operator mid-demo must never be handed a raw ImportError
                       traceback (tests/test_prod_demo.py pins the message)
```
