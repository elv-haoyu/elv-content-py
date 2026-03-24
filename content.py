"""Eluvio Content Fabric content object wrapper."""

from typing import Any, Dict

from elv_client_py import ElvClient


DEFAULT_CONFIG_URL = "https://main.net955305.contentfabric.io/config"


class Content:
    """Content object representation and API wrapper.

    Wraps an ElvClient and a resolved content object, automatically
    passing version_hash and library_id to proxied ElvClient calls.
    """

    def __init__(self, qhit: str, auth: str, config_url: str = DEFAULT_CONFIG_URL):
        client = ElvClient.from_configuration_url(
            config_url, static_token=auth
        )

        qinfo = client.content_object(**Content.parse_qhit(qhit))

        self.qid = qinfo["id"]
        self.qhash = qinfo["hash"]
        self.qlib = qinfo["qlib_id"]
        self._client = client
        self._token = auth

    def content_object_versions(self) -> Dict[str, Any]:
        """Get all versions of the content object."""
        return self._client.content_object_versions(
            object_id=self.qid, library_id=self.qlib
        )

    @property
    def fabric_node(self) -> str:
        """Return a fabric node URL."""
        return self._client.fabric_uris[0]

    def __getattr__(self, name):
        attr = getattr(self._client, name)
        if not callable(attr):
            raise AttributeError(
                f"'{name}' Content type does not have this attribute."
            )

        def wrapper(*args, **kwargs):
            return attr(
                *args, version_hash=self.qhash, library_id=self.qlib, **kwargs
            )

        return wrapper

    @staticmethod
    def parse_qhit(qhit: str) -> Dict[str, str]:
        """Parse a qhit into the correct kwarg for ElvClient methods."""
        if qhit.startswith("iq__"):
            return {"object_id": qhit}
        elif qhit.startswith("hq__"):
            return {"version_hash": qhit}
        elif qhit.startswith("tqw__"):
            return {"write_token": qhit}
        raise ValueError(f"Invalid qhit: {qhit}")
