# Changelog

All notable changes to `multiedge-relay` are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project adheres to [Semantic Versioning](https://semver.org/).

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
