"""One-time control-plane bootstrap for the two-terminal live demo.

Run with your tenant's ADMIN API key. Creates (or reuses) the demo strategy and a
demo subscriber client, registers a ``rest_pull`` endpoint, grants the entitlement,
and mints the two keys the demo terminals use:

* ``publisher:<strategy_id>`` — for ``producer_rebalance.py`` (Terminal 2)
* ``subscriber:<client_id>`` — for ``consumer_rebalance.py`` (Terminal 1)

Both keys are printed EXACTLY ONCE — the relay stores only their hashes and can
never re-reveal them. Copy them before closing the terminal.

Run:
    MULTIEDGE_ADMIN_KEY=mesk_... python setup_demo.py
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from typing import Any

import httpx

DEFAULT_BASE_URL = "https://relay-api.multiedge.ai"


@dataclass(frozen=True)
class BootstrapResult:
    """Identifiers and raw keys produced by one bootstrap run.

    The two ``*_key`` fields hold secret material shown once by the relay; the
    caller must hand them to the operator and never persist them anywhere else.
    """

    strategy_id: str
    client_id: str
    endpoint_id: str
    entitlement_id: str
    publisher_key: str
    subscriber_key: str


def _json(response: httpx.Response) -> dict[str, Any]:
    """Decode a JSON response body, raising loudly on unexpected statuses."""
    if response.status_code >= 400:
        raise RuntimeError(
            f"{response.request.method} {response.request.url.path} -> "
            f"{response.status_code}: {response.text}"
        )
    body: dict[str, Any] = response.json()
    return body


def _find_strategy_id(client: httpx.Client, slug: str) -> str:
    """Look up an existing strategy id by slug (used after a 409 on create)."""
    body = _json(client.get("/v1/strategies"))
    for strategy in body["strategies"]:
        if strategy["slug"] == slug:
            return str(strategy["strategy_id"])
    raise RuntimeError(f"slug '{slug}' conflicted on create but was not found in the listing")


def run_bootstrap(
    client: httpx.Client,
    *,
    slug: str,
    display_name: str,
    client_name: str,
    contact_email: str = "ops@example.invalid",
) -> BootstrapResult:
    """Create (or reuse) the demo strategy/client chain and mint the demo keys.

    Args:
        client: An ``httpx.Client`` with ``base_url`` set and the tenant ADMIN key
            in its Authorization header.
        slug: Strategy slug. Omitting ``signal_schema_json`` on create pins the
            relay's default ``portfolio_rebalance/1.0`` schema.
        display_name: Human-readable strategy name.
        client_name: Display name of the demo subscriber client; an existing
            client with this exact name is reused.
        contact_email: Contact recorded on a newly created client.

    Returns:
        The created/reused identifiers plus the freshly minted publisher and
        subscriber keys (shown once — treat as secrets).

    Note:
        Reruns reuse the strategy (by slug) and the client (by display name) but
        always mint fresh keys and register a fresh ``rest_pull`` endpoint; old
        keys can be revoked in the portal.
    """
    created = client.post(
        "/v1/strategies",
        json={
            "slug": slug,
            "display_name": display_name,
            "signal_schema_json": None,
            "compliance_profile": False,
        },
    )
    if created.status_code == 409:
        strategy_id = _find_strategy_id(client, slug)
    else:
        strategy_id = str(_json(created)["strategy_id"])

    clients = _json(client.get("/v1/clients"))["clients"]
    existing = [c for c in clients if c["display_name"] == client_name]
    if existing:
        client_id = str(existing[0]["client_id"])
    else:
        client_id = str(
            _json(
                client.post(
                    "/v1/clients",
                    json={"display_name": client_name, "primary_contact_email": contact_email},
                )
            )["client_id"]
        )

    endpoint_id = str(
        _json(
            client.post(
                f"/v1/clients/{client_id}/endpoints",
                json={"transport": "rest_pull", "url": None},
            )
        )["endpoint_id"]
    )

    entitlement = client.post(
        "/v1/entitlements",
        json={"client_id": client_id, "strategy_id": strategy_id, "policy_json": "{}"},
    )
    entitlement_id = (
        "(already granted)"
        if entitlement.status_code == 409
        else str(_json(entitlement)["entitlement_id"])
    )

    publisher_key = str(
        _json(client.post("/v1/api-keys", json={"scope": f"publisher:{strategy_id}"}))["api_key"]
    )
    subscriber_key = str(
        _json(client.post("/v1/api-keys", json={"scope": f"subscriber:{client_id}"}))["api_key"]
    )

    return BootstrapResult(
        strategy_id=strategy_id,
        client_id=client_id,
        endpoint_id=endpoint_id,
        entitlement_id=entitlement_id,
        publisher_key=publisher_key,
        subscriber_key=subscriber_key,
    )


def main() -> None:
    """Bootstrap the demo chain against the relay named by ``--base-url``."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--admin-key", default=os.environ.get("MULTIEDGE_ADMIN_KEY"))
    parser.add_argument("--slug", default="demo-rebalance")
    parser.add_argument("--display-name", default="Demo Rebalance")
    parser.add_argument("--client-name", default="Demo Subscriber Fund")
    args = parser.parse_args()

    if not args.admin_key:
        print("set MULTIEDGE_ADMIN_KEY or pass --admin-key mesk_...", file=sys.stderr)
        raise SystemExit(2)

    with httpx.Client(
        base_url=args.base_url,
        headers={"Authorization": f"Bearer {args.admin_key}"},
        timeout=30.0,
    ) as client:
        result = run_bootstrap(
            client,
            slug=args.slug,
            display_name=args.display_name,
            client_name=args.client_name,
        )

    print(f"strategy_id   = {result.strategy_id}   (slug: {args.slug})")
    print(f"client_id     = {result.client_id}")
    print(f"endpoint_id   = {result.endpoint_id}   (transport: rest_pull)")
    print(f"entitlement   = {result.entitlement_id}")
    print()
    print("=" * 72)
    print("!!  The two keys below are shown EXACTLY ONCE — copy them now.  !!")
    print("!!  The relay stores only hashes and cannot re-reveal them.     !!")
    print("=" * 72)
    print(f"PUBLISHER  key (Terminal 2 producer): {result.publisher_key}")
    print(f"SUBSCRIBER key (Terminal 1 consumer): {result.subscriber_key}")


if __name__ == "__main__":
    main()
