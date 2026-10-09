import importlib

from .config import DEFAULT_CONFIG_URL, fabric_nodes, resolve_config_url
from .elv_token import (create_token, elv_binary, elv_error, find_secret,
                        is_permission_error, load_token, resolve_token)
from .parts import PartDownloader, build_plan, select_streams


# Imported on first use: these need elv_client_py and loguru, which the token
# and part modules above do not.
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
