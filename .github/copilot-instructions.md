# GitHub Copilot instructions — multiedge-relay (Python SDK)

See [../CLAUDE.md](../CLAUDE.md) for the full engineering contract — single source of
truth for both agents, kept in sync in the same commit.

Non-negotiables: never-silent-loss error taxonomy; at-least-once subscriber with atomic
file cursor committed after the callback; catch-up/gap-fill ordered by relay `sequence`;
exactly-once *processing* via `SqliteStateStore` (marker committed atomically with
handler success; watermark + `signal_id` ledger invariant; loud `StateStoreCorruptError`,
never silent reset; never claim exactly-once *delivery*);
HMAC verify over `"{ts}." + raw_body` with constant-time compare and 5-min freshness;
TDD; `uv` tooling; ruff + black + mypy --strict green before "done"; runtime deps only
httpx + pydantic; public-repo hygiene (no internal business content or credentials).
