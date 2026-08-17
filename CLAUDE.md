# CLAUDE.md — multiedge-relay (Python SDK)

Official Python SDK for MultiEdge Signal Relay — auditable signal-distribution
infrastructure, not execution. Public repo, Apache 2.0, published to PyPI as
`multiedge-relay` (module `multiedge_relay`). Users are quant developers at publisher
firms and their institutional subscriber clients.

This file and `.github/copilot-instructions.md` are the SAME contract for two agents;
keep them in sync in the same commit.

## THE COMMANDS

```powershell
uv sync                                   # install (Python 3.12 dev default; supports 3.11–3.13)
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
   `ValidationRejected` (no retry), `PublishFailed` after 5 attempts (0.5·2ⁿ s + full
   jitter) carrying `dlq_path` when spilled to the disk DLQ (`~/.multiedge/dlq/*.jsonl`).
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
   the original ack for duplicates (`SignalAck.deduplicated=True`) — retries are safe.
5. **Webhook verification**: HMAC-SHA256 over `"{timestamp}." + raw_body` with the
   endpoint secret, `hmac.compare_digest`, reject |now − ts| > 5 min, injectable clock.
   Verify raw received bytes — never re-serialize.
6. **TDD**; runtime deps only `httpx` + `pydantic` (extras: `[webpubsub]`); mypy --strict;
   Google docstrings on every public class/function; py.typed shipped; frozen pydantic v2
   models; SemVer; CHANGELOG kept current.
7. **Public repo hygiene:** no business plans, internal strategy, credentials, or
   Azure resource details in this repo. Examples use placeholder keys and
   `https://relay-api.multiedge.ai`.
8. **Latest supported versions.** Dependencies and the Python matrix ride the newest
   released versions the toolchain supports; verify ceilings, never guess, and record
   any forced pin with its reason here.

## Layout

```text
src/multiedge_relay/   models, envelope, ulid, exceptions, _retry, _http,
                       publisher, publisher_async, dlq, cursor, subscriber,
                       _webpubsub (extra), webhook, cli
tests/                 fake_relay.py (in-proc FastAPI via httpx.ASGITransport) + suites;
                       markers: integration
examples/              publish_minimal, publish_batch_with_dlq, subscribe_catchup,
                       webhook_fastapi, webhook_flask, publish_rebalance_from_csv
```
