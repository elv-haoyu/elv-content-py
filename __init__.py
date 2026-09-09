"""Python helpers for the Eluvio content fabric.

Two download paths, one auth path:

    PartDownloader      whole objects, part by part, through the `elv` CLI
    ContentDownloader   time ranges, transcoded by the media/files API
    TitleExtractor      title metadata off the `public` subtree
    create_token        fabric auth tokens, signed by the `elv` CLI

Only `requests` is required. `Content`, `ContentDownloader` and `TitleExtractor`
additionally need `elv_client_py` and `loguru`, so they are imported on first
use -- the token and part modules work without them installed.
"""

import importlib

from .config import DEFAULT_CONFIG_URL, fabric_nodes, resolve_config_url
from .elv_token import (create_token, elv_binary, elv_error, find_secret,
                        is_permission_error, load_token, resolve_token)
from .parts import PartDownloader, build_plan, select_streams


_LAZY = {
    "Content": "content",
    "ContentDownloader": "downloader",
    "TitleExtractor": "extractor",
    "parse_title_metadata": "extractor",
}


def __getattr__(name):
    module = _LAZY.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return getattr(importlib.import_module(f".{module}", __name__), name)


def __dir__():
    return sorted(__all__)


__all__ = [
    "Content",
    "ContentDownloader",
    "DEFAULT_CONFIG_URL",
    "PartDownloader",
    "TitleExtractor",
    "build_plan",
    "create_token",
    "elv_binary",
    "elv_error",
    "fabric_nodes",
    "find_secret",
    "is_permission_error",
    "load_token",
    "parse_title_metadata",
    "resolve_config_url",
    "resolve_token",
    "select_streams",
]
