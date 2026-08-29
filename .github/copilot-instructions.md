# GitHub Copilot instructions — multiedge-relay (Python SDK)

See [../CLAUDE.md](../CLAUDE.md) for the full engineering contract — single source of
truth for both agents, kept in sync in the same commit.

Non-negotiables: never-silent-loss error taxonomy; at-least-once subscriber with atomic
file cursor committed after the callback; catch-up/gap-fill ordered by relay `sequence`;
one shared `RetryPolicy` (`_retry.py`) for all three call sites, bounded by WALL CLOCK
(publishers: `retry_budget_seconds=90`, long enough to ride out a relay deployment;
subscriber catch-up: unbounded, bounded by `stop()`, every attempt reported via
`on_error`), 8 s per-sleep cap, `Retry-After` honoured, terminal statuses never retried;
exactly-once *processing* via `SqliteStateStore` (marker committed atomically with
handler success; watermark + `signal_id` ledger invariant; loud `StateStoreCorruptError`,
never silent reset; never claim exactly-once *delivery*);
HMAC verify over `"{ts}." + raw_body` with constant-time compare and 5-min freshness;
TDD; `uv` tooling; ruff + black + mypy --strict green before "done"; runtime deps only
httpx + pydantic (extras: `[webpubsub]`, `[sealed]`); public-repo hygiene (no internal
business content or credentials).

Sealed mode (relay ADR 0004): all crypto lives in `multiedge_relay/sealed/` behind the
`[sealed]` extra (`cryptography>=47`); core NEVER imports it (CI core-only job proves
it). The sealed envelope v1 wire format is FROZEN by the committed vector
`tests/fixtures/sealed_v1_vector.json` — changes require `sealed: "v2"` + a new vector.
Sealing happens in `prepare_signal` after ULID assignment and before DLQ spill; fetched
key bundles are always re-fingerprinted locally (the relay is untrusted for key
authenticity); never log or serialize private key material.
