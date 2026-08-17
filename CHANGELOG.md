# Changelog

All notable changes to `multiedge-relay` are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project adheres to [Semantic Versioning](https://semver.org/).

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
