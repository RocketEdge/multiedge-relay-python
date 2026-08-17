"""In-process fake MultiEdge Signal Relay for tests.

A FastAPI app with an in-memory, per-strategy sequenced signal store, served to the
SDK through httpx transports (``httpx.ASGITransport`` for async clients and
``SyncASGITransport`` — a thin sync driver over the same ASGI app — for sync clients).

Knobs:
    * ``fail_next(n, status)`` — the next *n* requests return *status*.
    * ``inject_gap(strategy_id, seqs)`` — remove stored sequences (simulates data the
      relay can no longer serve, for gap-unrecoverable tests).
    * ``page_size_cap`` — server-side maximum page size.
"""

from __future__ import annotations

import asyncio
import itertools
import threading
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

API_KEY = "test-api-key"


@dataclass
class StoredSignal:
    """One accepted signal in the fake relay's per-strategy log."""

    sequence: int
    signal_id: str
    strategy_id: str
    client_signal_id: str
    published_at: datetime
    payload: dict[str, Any]

    def as_received(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "signal_id": self.signal_id,
            "strategy_id": self.strategy_id,
            "client_signal_id": self.client_signal_id,
            "published_at": self.published_at.isoformat(),
            "payload": self.payload,
        }

    def as_ack(self, *, deduplicated: bool) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "client_signal_id": self.client_signal_id,
            "sequence": self.sequence,
            "accepted_at": self.published_at.isoformat(),
            "deduplicated": deduplicated,
        }


@dataclass
class FakeRelay:
    """In-memory relay backend + FastAPI app factory."""

    page_size_cap: int = 1000
    signals: dict[str, list[StoredSignal]] = field(default_factory=dict)
    requests: list[str] = field(default_factory=list)
    # Sealed-mode key registry (ADR 0004): per-strategy recipient/sender bundles,
    # stored opaquely as {"key_id": ..., "bundle": ...} — like the real relay.
    recipient_keys: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    sender_keys: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._seq: dict[str, int] = {}
        self._by_client_id: dict[tuple[str, str], StoredSignal] = {}
        self._fail_queue: list[int] = []
        self._id_counter = itertools.count(1)
        self._lock = threading.Lock()
        self.app = self._build_app()

    # ------------------------------------------------------------------ knobs
    def fail_next(self, n: int, status: int) -> None:
        """Make the next *n* HTTP requests fail with *status*."""
        self._fail_queue.extend([status] * n)

    def inject_gap(self, strategy_id: str, seqs: Iterable[int]) -> None:
        """Remove the given sequences from the store (unrecoverable gap)."""
        drop = set(seqs)
        self.signals[strategy_id] = [
            s for s in self.signals.get(strategy_id, []) if s.sequence not in drop
        ]

    def seed(self, strategy_id: str, payloads: Iterable[dict[str, Any]]) -> list[StoredSignal]:
        """Directly append accepted signals (no HTTP), returning the stored rows."""
        return [self._store(strategy_id, payload, client_signal_id=None) for payload in payloads]

    # ------------------------------------------------------------------ store
    def _store(
        self, strategy_id: str, payload: dict[str, Any], client_signal_id: str | None
    ) -> StoredSignal:
        with self._lock:
            seq = self._seq.get(strategy_id, 0) + 1
            self._seq[strategy_id] = seq
            n = next(self._id_counter)
            stored = StoredSignal(
                sequence=seq,
                signal_id=f"sig_{strategy_id}_{n:06d}",
                strategy_id=strategy_id,
                client_signal_id=client_signal_id or f"auto_{n:06d}",
                published_at=datetime.now(UTC),
                payload=payload,
            )
            self.signals.setdefault(strategy_id, []).append(stored)
            if client_signal_id is not None:
                self._by_client_id[(strategy_id, client_signal_id)] = stored
            return stored

    # ------------------------------------------------------------------ app
    def _build_app(self) -> FastAPI:
        app = FastAPI()

        @app.middleware("http")
        async def knobs_and_auth(request: Request, call_next: Any) -> Any:
            self.requests.append(f"{request.method} {request.url.path}")
            if self._fail_queue:
                status = self._fail_queue.pop(0)
                return JSONResponse({"error": "injected failure"}, status_code=status)
            auth = request.headers.get("authorization", "")
            if auth != f"Bearer {API_KEY}":
                return JSONResponse({"error": "invalid api key"}, status_code=401)
            return await call_next(request)

        @app.post("/v1/signals")
        async def publish(request: Request) -> JSONResponse:
            body = await request.json()
            strategy_id = body.get("strategy_id")
            payload = body.get("payload")
            if not strategy_id or not isinstance(payload, dict):
                return JSONResponse({"error": "invalid signal"}, status_code=422)
            client_signal_id = body.get("client_signal_id")
            if client_signal_id:
                existing = self._by_client_id.get((strategy_id, client_signal_id))
                if existing is not None:
                    return JSONResponse(existing.as_ack(deduplicated=True), status_code=200)
            stored = self._store(strategy_id, payload, client_signal_id)
            return JSONResponse(stored.as_ack(deduplicated=False), status_code=201)

        @app.get("/v1/signals")
        async def list_signals(
            strategy_id: str, since_sequence: int = 0, limit: int = 100
        ) -> JSONResponse:
            limit = min(limit, self.page_size_cap)
            rows = [
                s.as_received()
                for s in self.signals.get(strategy_id, [])
                if s.sequence > since_sequence
            ]
            rows.sort(key=lambda r: int(r["sequence"]))
            return JSONResponse({"signals": rows[:limit]})

        @app.post("/v1/ws/negotiate")
        async def negotiate(request: Request) -> JSONResponse:
            return JSONResponse(
                {"url": "wss://example.invalid/client/hubs/signals?access_token=fake"}
            )

        @app.get("/v1/strategies/{strategy_id}/sealed-keys")
        async def list_recipient_keys(strategy_id: str) -> JSONResponse:
            return JSONResponse({"keys": self.recipient_keys.get(strategy_id, [])})

        @app.get("/v1/strategies/{strategy_id}/sealed-keys/sender")
        async def get_sender_keys(strategy_id: str) -> JSONResponse:
            return JSONResponse({"keys": self.sender_keys.get(strategy_id, [])})

        @app.post("/v1/clients/{client_id}/sealed-keys")
        async def register_recipient_key(client_id: str, request: Request) -> JSONResponse:
            body = await request.json()
            entry = {"key_id": body["key_id"], "client_id": client_id, "bundle": body["bundle"]}
            # The fake has no entitlement model: register under the strategy named
            # by the test via the client id convention "strategy:<sid>", else "any".
            strategy_id = client_id.removeprefix("strategy:") if ":" in client_id else "any"
            self.recipient_keys.setdefault(strategy_id, []).append(entry)
            return JSONResponse(entry, status_code=201)

        @app.put("/v1/strategies/{strategy_id}/sealed-keys/sender")
        async def register_sender_key(strategy_id: str, request: Request) -> JSONResponse:
            body = await request.json()
            entry = {"key_id": body["key_id"], "strategy_id": strategy_id, "bundle": body["bundle"]}
            self.sender_keys.setdefault(strategy_id, []).insert(0, entry)
            return JSONResponse(entry, status_code=201)

        return app


class SyncASGITransport(httpx.BaseTransport):
    """Drive an ASGI app from a synchronous ``httpx.Client`` (test-only)."""

    def __init__(self, app: Any) -> None:
        self._asgi = httpx.ASGITransport(app=app)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        content = request.read()

        async def _go() -> httpx.Response:
            async_request = httpx.Request(
                request.method, request.url, headers=request.headers, content=content
            )
            response = await self._asgi.handle_async_request(async_request)
            body = await response.aread()
            await response.aclose()
            return httpx.Response(
                response.status_code,
                headers=response.headers,
                content=body,
                request=request,
            )

        return asyncio.run(_go())
