"""httpx client factories with the SDK's auth, UA, and timeout conventions.

Purpose:
    One place that knows how to build a relay-ready ``httpx.Client`` /
    ``httpx.AsyncClient``: Bearer auth, versioned User-Agent, 10 s default timeout,
    and a ``transport`` override seam so tests can route requests to an in-process
    ASGI app or a respx mock without touching the network.
"""

from __future__ import annotations

import httpx

from . import __version__

USER_AGENT = f"multiedge-relay-python/{__version__}"

DEFAULT_BASE_URL = "https://api.multiedge.com"
DEFAULT_TIMEOUT_SECONDS = 10.0


def _headers(api_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key}",
        "User-Agent": USER_AGENT,
    }


def build_client(
    api_key: str,
    base_url: str = DEFAULT_BASE_URL,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    transport: httpx.BaseTransport | None = None,
) -> httpx.Client:
    """Build a synchronous relay client.

    Args:
        api_key: Relay API key, sent as ``Authorization: Bearer <key>``.
        base_url: Relay origin; override for staging or self-hosted deployments.
        timeout: Per-request timeout in seconds (connect + read + write + pool).
        transport: Optional transport override (test seam); ``None`` uses the default
            network transport.

    Returns:
        A configured ``httpx.Client``. Caller owns closing it.
    """
    return httpx.Client(
        base_url=base_url,
        headers=_headers(api_key),
        timeout=timeout,
        transport=transport,
    )


def build_async_client(
    api_key: str,
    base_url: str = DEFAULT_BASE_URL,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    transport: httpx.AsyncBaseTransport | None = None,
) -> httpx.AsyncClient:
    """Build an asynchronous relay client; see :func:`build_client` for parameters."""
    return httpx.AsyncClient(
        base_url=base_url,
        headers=_headers(api_key),
        timeout=timeout,
        transport=transport,
    )
