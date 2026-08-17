"""FastAPI webhook receiver with HMAC verification.

Verify the RAW request bytes — never a re-parsed/re-serialized body.

Run:
    MULTIEDGE_WEBHOOK_SECRET=whsec_your_endpoint_secret uvicorn webhook_fastapi:app
"""

from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException, Request

from multiedge_relay import SignatureVerificationError, SqliteStateStore, verify_signature

app = FastAPI()
WEBHOOK_SECRET = os.environ.get("MULTIEDGE_WEBHOOK_SECRET", "whsec_your_endpoint_secret")
STATE = SqliteStateStore()  # ~/.multiedge/state.db — dedups the relay's retries/replays


@app.post("/webhooks/multiedge")
async def receive_signal(request: Request) -> dict[str, object]:
    """Verify and process one webhook delivery exactly once.

    The relay retries deliveries that do not return 2xx (and operators can
    replay), so the same ``signal_id`` can arrive again — the state store makes
    the handler body run at most once per signal. Duplicates are still answered
    2xx so the retry ladder stops.
    """
    raw_body = await request.body()
    try:
        signal = verify_signature(raw_body, dict(request.headers), WEBHOOK_SECRET)
    except SignatureVerificationError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc

    with STATE.process(signal) as fresh:
        if fresh:
            print(f"verified seq={signal.sequence} payload={signal.payload}")
    return {"ok": True, "sequence": signal.sequence, "duplicate": not fresh}
