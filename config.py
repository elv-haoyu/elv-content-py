"""Fabric configuration lookup.

Kept free of every optional dependency (no elv_client_py, no loguru) so the
token and part-download modules can be used on their own.
"""

import os
from functools import lru_cache

import requests


DEFAULT_CONFIG_URL = "https://main.net955305.contentfabric.io/config"
CONFIG_URL_ENV = "ELV_CONFIG_URL"
TIMEOUT = 60


def resolve_config_url(config_url: str | None = None) -> str:
    """Explicit argument wins, then $ELV_CONFIG_URL, then the main network.

    A single-node config (``https://host-<ip>.contentfabric.io/config?self&qspace=main``)
    is a valid value here and pins every request to that node.
    """
    return config_url or os.environ.get(CONFIG_URL_ENV) or DEFAULT_CONFIG_URL


@lru_cache(maxsize=16)
def fabric_nodes(config_url: str | None = None) -> tuple[str, ...]:
    """Fabric API endpoints advertised by the config URL, in its own order."""
    url = resolve_config_url(config_url)
    response = requests.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    services = response.json().get("network", {}).get("services", {})
    nodes = services.get("fabric_api") or []
    if not nodes:
        raise RuntimeError(f"no fabric_api endpoints advertised by {url}")
    return tuple(node.rstrip("/") for node in nodes)
