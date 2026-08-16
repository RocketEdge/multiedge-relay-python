"""Flask webhook receiver with HMAC verification.

Verify the RAW request bytes (``request.get_data()``) — never ``request.json``
re-serialized.

Run:
    pip install flask
    MULTIEDGE_WEBHOOK_SECRET=whsec_your_endpoint_secret flask --app webhook_flask run
"""

from __future__ import annotations

import os

from flask import Flask, jsonify, request

from multiedge_relay import SignatureVerificationError, verify_signature

app = Flask(__name__)
WEBHOOK_SECRET = os.environ.get("MULTIEDGE_WEBHOOK_SECRET", "whsec_your_endpoint_secret")


@app.post("/webhooks/multiedge")
def receive_signal():  # type: ignore[no-untyped-def]  # Flask view
    """Verify and process one webhook delivery (idempotently — the relay retries)."""
    raw_body = request.get_data()  # raw bytes, exactly as received
    try:
        signal = verify_signature(raw_body, dict(request.headers), WEBHOOK_SECRET)
    except SignatureVerificationError as exc:
        return jsonify({"error": str(exc)}), 401

    # ... idempotent processing here ...
    print(f"verified seq={signal.sequence} payload={signal.payload}")
    return jsonify({"ok": True, "sequence": signal.sequence})
