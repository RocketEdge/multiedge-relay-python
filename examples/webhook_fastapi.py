"""FastAPI webhook receiver with HMAC verification.

Verify the RAW request bytes — never a re-parsed/re-serialized body.

Run:
    MULTIEDGE_WEBHOOK_SECRET=whsec_your_endpoint_secret uvicorn webhook_fastapi:app
"""

from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException, Request

from multiedge_relay import SignatureVerificationError, verify_signature

app = FastAPI()
WEBHOOK_SECRET = os.environ.get("MULTIEDGE_WEBHOOK_SECRET", "whsec_your_endpoint_secret")


@app.post("/webhooks/multiedge")
async def receive_signal(request: Request) -> dict[str, object]:
    """Verify and process one webhook delivery.

    Processing must be idempotent (key on ``signal.signal_id``): the relay retries
    deliveries that do not return 2xx.
    """
    raw_body = await request.body()
    try:
        signal = verify_signature(raw_body, dict(request.headers), WEBHOOK_SECRET)
    except SignatureVerificationError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc

    # ... idempotent processing here ...
    print(f"verified seq={signal.sequence} payload={signal.payload}")
    return {"ok": True, "sequence": signal.sequence}
