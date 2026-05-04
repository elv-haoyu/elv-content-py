import json
from pathlib import Path
from typing import Dict, List, Optional

from loguru import logger

from .content import Content, DEFAULT_CONFIG_URL

EXPECTED_FIELDS = [
    "display_title", "release_date", "release_year",
    "title_type", "plot", "cast", "director",
]


# ---------------------------------------------------------------------------
# Name / cast helpers
# ---------------------------------------------------------------------------

def _full_name(entry: dict) -> str:
    """Build full name from either {name} or {talent_first_name, talent_last_name}."""
    if entry.get("name"):
        return entry["name"]
    first = (entry.get("talent_first_name") or "").strip()
    last = (entry.get("talent_last_name") or "").strip()
    return f"{first} {last}".strip()


def _extract_cast_pattern1(talent: dict) -> list:
    """Pattern 1: talent.actor / talent.voice / talent.host with {name}."""
    cast = []
    for a in talent.get("actor") or []:
        name = _full_name(a)
        if not name:
            continue
        if a.get("character_name"):
            cast.append(f"{name} plays {a['character_name']}")
        else:
            cast.append(name)
    for v in talent.get("voice") or []:
        name = _full_name(v)
        if not name:
            continue
        if v.get("character_name"):
            cast.append(f"{name} voices {v['character_name']}")
        else:
            cast.append(name)
    for h in talent.get("host") or []:
        name = _full_name(h)
        if name:
            cast.append(f"{name} (Host)")
    return cast


def _extract_cast_pattern2(talent: dict) -> list:
    """Pattern 2 (MGM): talent.cast / talent.cast_mdb with
    {talent_first_name, talent_last_name, character_name}."""
    cast = []
    entries = talent.get("cast") or talent.get("cast_mdb") or []
    for entry in entries:
        name = _full_name(entry)
        if not name:
            continue
        char = (entry.get("character_name") or "").strip()
        if char:
            cast.append(f"{name} plays {char}")
        else:
            cast.append(name)
    return cast


def _extract_cast_from_ml(client: Content) -> list:
    """Pattern 3 (ml subtree): v0.content_params.castlist as {actor: character}."""
    try:
        ml = client.content_object_metadata(metadata_subtree='ml')
        castlist = (ml.get("v0") or {}).get(
            "content_params", {}).get("castlist") or {}
        cast = []
        for actor, character in castlist.items():
            actor = actor.strip()
            character = (character or "").strip()
            if character and character != actor:
                cast.append(f"{actor} plays {character}")
            elif actor:
                cast.append(actor)
        return cast
    except Exception:
        return []


def _extract_directors(talent: dict) -> list:
    """Extract director names from either pattern."""
    directors = []
    for d in talent.get("director") or talent.get("directors") or []:
        name = _full_name(d)
        if name:
            directors.append(name)
    return directors


def _extract_screenplay(talent: dict) -> list | None:
    """Extract screenplay/writer credits from either pattern."""
    seen = set()
    for key in ("screenplay_by", "written_by", "screenplay"):
        for entry in talent.get(key) or []:
            name = _full_name(entry) if isinstance(entry, dict) else entry
            if name and name not in seen:
                seen.add(name)
    return list(seen) or None


# ---------------------------------------------------------------------------
# Core parsing
# ---------------------------------------------------------------------------

def parse_title_metadata(metadata: dict, client: Optional[Content] = None) -> dict:
    """Parse raw fabric metadata into a normalized title info dict.

    Args:
        metadata: Raw fabric metadata (from ``public`` subtree).
        client: Optional Content client, used for ml-subtree cast fallback.

    Returns:
        Dict with non-empty fields only.
    """
    asset = metadata.get("asset_metadata") or {}
    info = asset.get("info") or {}
    talent = info.get("talent") or {}

    # Try pattern 1 (actor/voice/host), then pattern 2 (cast/cast_mdb),
    # then pattern 3 (ml subtree castlist)
    cast = _extract_cast_pattern1(talent) or _extract_cast_pattern2(talent)
    if not cast and client is not None:
        cast = _extract_cast_from_ml(client)

    # Synopsis: prefer info.synopsis, fall back to asset_metadata.synopsis
    plot = info.get("synopsis") or asset.get("synopsis")

    fields = {
        "display_title": asset.get("display_title"),
        "release_date":  info.get("release_date"),
        "release_year":  info.get("us_release_year"),
        "title_type":    asset.get("title_type", ""),
        "plot":          plot,
        "cast":          cast or None,
        "director":      _extract_directors(talent),
        "screenplay":    _extract_screenplay(talent),
    }

    return {k: v for k, v in fields.items() if v}


# ---------------------------------------------------------------------------
# Per-QID file cache
# ---------------------------------------------------------------------------

# In-memory cache to avoid repeated disk reads on hot paths
_title_cache: dict[str, dict] = {}


def title_path(metadata_dir: Path, qid: str) -> Path:
    return metadata_dir / f"{qid}_title.json"


def load_title_info_for_qid(metadata_dir: Path, qid: str) -> dict | None:
    """Load cached title info for a single content from metadata/{qid}_title.json."""
    if qid in _title_cache:
        return _title_cache[qid]
    path = title_path(metadata_dir, qid)
    if not path.exists():
        return None
    with open(path) as f:
        info = json.load(f)
    _title_cache[qid] = info
    return info


def load_all_title_info(metadata_dir: Path) -> Dict:
    """Load all cached title info files into a single dict keyed by QID."""
    result = {}
    if not metadata_dir.exists():
        return result
    for path in metadata_dir.glob("*_title.json"):
        qid = path.name.removesuffix("_title.json")
        try:
            with open(path) as f:
                info = json.load(f)
            result[qid] = info
            _title_cache[qid] = info
        except Exception:
            logger.warning(f"Failed to load {path}")
    return result


def save_title_info_for_qid(metadata_dir: Path, qid: str, info: dict):
    """Save title info for a single content to metadata/{qid}_title.json."""
    metadata_dir.mkdir(parents=True, exist_ok=True)
    path = title_path(metadata_dir, qid)
    with open(path, "w") as f:
        json.dump(info, f, indent=2)
    _title_cache[qid] = info


# ---------------------------------------------------------------------------
# Field validation / error logging
# ---------------------------------------------------------------------------

def check_title_fields(qid: str, info: dict,
                       error_log: Optional[Path] = None) -> list[str]:
    """Check for missing expected fields and log warnings.

    Args:
        qid: Content object ID.
        info: Title info dict (may be empty).
        error_log: Optional path to append error entries to.

    Returns:
        List of missing field names.
    """
    if not info:
        msg = "no title metadata"
    else:
        missing = [f for f in EXPECTED_FIELDS if f not in info]
        if not missing:
            return []
        msg = f"title_info missing fields: {', '.join(missing)}"

    logger.warning(f"{qid}: {msg}")
    if error_log:
        error_log.parent.mkdir(parents=True, exist_ok=True)
        with open(error_log, "a") as f:
            f.write(f"{qid}: {msg}\n")
    return list(EXPECTED_FIELDS) if not info else missing


# ---------------------------------------------------------------------------
# TitleExtractor class (high-level API)
# ---------------------------------------------------------------------------

class TitleExtractor:
    """Extract and cache title metadata from Eluvio content fabric objects.

    Stores per-QID JSON files under ``metadata_dir`` and maintains an
    in-memory cache for fast repeated lookups.
    """

    def __init__(self, auth_token: str,
                 metadata_dir: Path = Path("metadata"),
                 config_url: str = DEFAULT_CONFIG_URL):
        self._token = auth_token
        self._config_url = config_url
        self.metadata_dir = metadata_dir
        self.error_log = metadata_dir / "error_title.log"

    def _get_content(self, qhit: str) -> Content:
        return Content(qhit, self._token, self._config_url)

    # --- single QID ---

    def extract(self, qhit: str) -> dict:
        """Extract title information for a single content object (always hits fabric)."""
        content = self._get_content(qhit)
        metadata = content.content_object_metadata(metadata_subtree="public")
        result = parse_title_metadata(metadata, client=content)
        if result:
            save_title_info_for_qid(self.metadata_dir, content.qid, result)
        return result

    def ensure(self, qid: str, force: bool = False) -> dict:
        """Return cached title info, fetching from fabric only if missing."""
        if not force:
            cached = load_title_info_for_qid(self.metadata_dir, qid)
            if cached is not None:
                logger.info(f"{qid}: title_info already cached")
                return cached
        else:
            title_path(self.metadata_dir, qid).unlink(missing_ok=True)
            _title_cache.pop(qid, None)

        logger.info(f"{qid}: fetching title_info from fabric")
        return self.extract(qid)

    # --- batch ---

    def extract_batch(self, qhits: List[str]) -> Dict[str, dict]:
        """Extract title information for multiple content objects."""
        results = {}
        for qhit in qhits:
            content = self._get_content(qhit)
            metadata = content.content_object_metadata(
                metadata_subtree="public")
            info = parse_title_metadata(metadata, client=content)
            if info:
                save_title_info_for_qid(self.metadata_dir, content.qid, info)
            results[content.qid] = info
        return results

    # --- cache access ---

    def load(self, qid: str) -> dict | None:
        return load_title_info_for_qid(self.metadata_dir, qid)

    def load_all(self) -> Dict:
        return load_all_title_info(self.metadata_dir)

    def check(self, qid: str, info: dict) -> list[str]:
        return check_title_fields(qid, info, error_log=self.error_log)
