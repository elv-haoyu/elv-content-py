import json
from pathlib import Path
from typing import Dict, List

from .content import Content, DEFAULT_CONFIG_URL


class TitleExtractor:
    """Extract title metadata from Eluvio content fabric objects."""

    FIELDS = [
        "display_title", "release_date", "release_year",
        "plot", "actor", "director", "screenplay",
    ]

    def __init__(self, auth_token: str, config_url: str = DEFAULT_CONFIG_URL):
        self._token = auth_token
        self._config_url = config_url

    def _get_content(self, qhit: str) -> Content:
        return Content(qhit, self._token, self._config_url)

    @staticmethod
    def _parse_title_info(metadata: dict) -> dict:
        """Parse raw fabric metadata into a normalized title info dict."""
        asset = metadata.get("asset_metadata") or {}
        info = asset.get("info") or {}
        talent = info.get("talent") or {}

        # Actors
        actor_info = talent.get("actor") or []
        actors = []
        if actor_info:
            actors = [
                f"{a['name']} plays {a['character_name']}"
                for a in actor_info
                if a.get("name") and a.get("character_name")
            ]

        # Directors
        director_info = talent.get("director") or []
        directors = [d["name"] for d in director_info if d.get("name")]

        # Screenplay / written by (deduplicated)
        screenplay_by = talent.get("screenplay_by") or []
        written_by = talent.get("written_by") or []
        seen = set()
        for entry in [*screenplay_by, *written_by]:
            key = entry.get("name") if isinstance(entry, dict) else entry
            if key and key not in seen:
                seen.add(key)
        screenplay = list(seen) or None

        fields = {
            "display_title": asset.get("display_title"),
            "release_date": info.get("release_date"),
            "release_year": info.get("us_release_year"),
            "plot": info.get("synopsis"),
            "actor": actors,
            "director": directors,
            "screenplay": screenplay,
        }

        return {k: v for k, v in fields.items() if v}

    def extract(self, qhit: str) -> dict:
        """Extract title information for a single content object.

        Args:
            qhit: Content object ID (iq__...) or version hash (hq__...).

        Returns:
            Dict with keys: display_title, release_date, release_year,
            plot, actor, director, screenplay (only non-empty fields).
        """
        content = self._get_content(qhit)
        metadata = content.content_object_metadata(metadata_subtree="public")
        return self._parse_title_info(metadata)

    def extract_batch(self, qhits: List[str]) -> Dict[str, dict]:
        """Extract title information for multiple content objects.

        Args:
            qhits: List of content object IDs or version hashes.

        Returns:
            Dict mapping each qid to its title info.
        """
        results = {}
        for qhit in qhits:
            content = self._get_content(qhit)
            metadata = content.content_object_metadata(metadata_subtree="public")
            results[content.qid] = self._parse_title_info(metadata)
        return results

    @staticmethod
    def load(path: str) -> dict:
        """Load title info from a JSON file."""
        p = Path(path)
        if not p.exists():
            return {}
        with open(p, "r") as f:
            return json.load(f)

    @staticmethod
    def save(title_info: dict, path: str):
        """Save title info to a JSON file."""
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w") as f:
            json.dump(title_info, f, indent=2)
