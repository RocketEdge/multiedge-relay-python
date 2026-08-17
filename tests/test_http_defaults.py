"""Default-endpoint contract for the SDK.

The relay's public hostname follows the MultiEdge per-product convention
(``<product>-api.multiedge.ai``); ``api.multiedge.ai`` is reserved for a
future shared gateway and must never be a product default (relay repo
``docs/architecture.md`` §Domains).
"""

from multiedge_relay._http import DEFAULT_BASE_URL


def test_default_base_url_is_the_relay_product_hostname() -> None:
    assert DEFAULT_BASE_URL == "https://relay-api.multiedge.ai"
