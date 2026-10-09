import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterator
from urllib.parse import urljoin

import requests

from elv_client_py import ElvClient

from .config import DEFAULT_CONFIG_URL

logger = logging.getLogger(__name__)


class TrimmedOfferingError(ValueError):
    """Every usable offering is a trim of the mezzanine.

    Its playout timeline starts at the offering's entry point, while the
    Tagstore tracks (auto_captions, speech_to_text, celebrity_detection) are
    on the mezzanine timeline that starts at 0, so audio downloaded from it
    would be offset from every tag.
    """


@dataclass(frozen=True)
class Playout:
    """One (offering, playout format) pair and where its timeline starts."""

    offering: str
    key: str
    manifest_url: str
    # Mezzanine seconds at which this offering's playout starts: 0.0 when the
    # offering has no entry point, None when entry_point_rat was present but
    # unreadable, which counts as trimmed until proven otherwise.
    entry_s: float | None
    # Mezzanine seconds at which the playout ends, or None when untrimmed.
    exit_s: float | None

    @property
    def trimmed(self) -> bool:
        """True when the playout timeline does not start at mezzanine 0."""
        return self.entry_s is None or self.entry_s > 0

    def describe(self) -> str:
        entry = "unreadable" if self.entry_s is None else f"{self.entry_s:.3f}s"
        exit_ = "none" if self.exit_s is None else f"{self.exit_s:.3f}s"
        return (f"{self.offering} ({self.key}) entry_point={entry} "
                f"exit_point={exit_}")


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

    def available_offerings(self) -> list[str]:
        """Return the list of playout offering keys from content metadata."""
        meta = self._client.content_object_metadata(
            version_hash=self.qhash,
            library_id=self.qlib,
            metadata_subtree="/offerings",
        )
        return list(meta) if isinstance(meta, dict) else []

    # Playout keys in preference order — DASH first, then HLS
    _PLAYOUT_KEYS = (
        "dash-clear", "dash-widevine", "dash",
        "hls-clear", "hls-sample-aes", "hls-aes128", "hls-fairplay", "hls",
    )
    _CLEAR_PLAYOUT_KEYS = ("dash-clear", "hls-clear")
    # HLS playouts whose audio ffmpeg can fetch directly, most preferred
    # first.  AES-128 segments are whole-segment encrypted with a key served
    # over HTTP, which ffmpeg decrypts natively — unlike sample-aes, fairplay
    # and widevine, which need a DRM license.
    _FFMPEG_HLS_KEYS = ("hls-clear", "hls-aes128")
    # Audio track preference, most preferred first
    _AUDIO_PREFS = ("english_5_1", "english_stereo")

    @staticmethod
    def _rat_seconds(value: Any) -> float | None:
        """Seconds from a fabric rational ('2162160/24000') or a number.

        Returns None when the value is present but not readable as either.
        """
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            text = value.strip()
            if "/" in text:
                num, _, den = text.partition("/")
                try:
                    return int(num) / int(den)
                except (ValueError, ZeroDivisionError):
                    return None
            try:
                return float(text)
            except ValueError:
                return None
        return None

    def offering_trim(self, offering: str) -> tuple[float | None, float | None]:
        """(entry_s, exit_s) of an offering, in mezzanine seconds.

        An offering with ``entry_point_rat`` / ``exit_point_rat`` is a trim of
        the mezzanine: its playout starts at the entry point, while the
        Tagstore tracks start at mezzanine 0. Links (``default_clear`` ->
        ``default``) are resolved server-side. Absent keys mean untrimmed,
        (0.0, None); a present but unreadable entry point comes back as None,
        which Playout counts as trimmed.
        """
        # Kept in __dict__: a missing attribute would reach __getattr__,
        # which proxies ElvClient.
        cache = self.__dict__.setdefault("_trim_cache", {})
        if offering not in cache:
            values: list[float | None] = []
            for key in ("entry_point_rat", "exit_point_rat"):
                raw = self._client.content_object_metadata(
                    version_hash=self.qhash,
                    library_id=self.qlib,
                    metadata_subtree=f"/offerings/{offering}/{key}",
                    resolve_links=True,
                )
                if raw is None:
                    values.append(0.0 if key == "entry_point_rat" else None)
                    continue
                seconds = self._rat_seconds(raw)
                if seconds is None:
                    logger.warning(
                        "Offering %s: unreadable %s=%r; treating it as trimmed",
                        offering, key, raw,
                    )
                values.append(seconds)
            cache[offering] = (values[0], values[1])
        return cache[offering]

    def _iter_playouts(
        self, candidates: list[str], keys: tuple[str, ...],
    ) -> Iterator[Playout]:
        """Every (offering, key) with a playout URI, in candidate order.

        Fetches options.json once per offering and the trim points once per
        offering that has at least one matching playout.
        """
        for name in candidates:
            try:
                opts = self.playout_options(name)
            except Exception:
                continue
            trim: tuple[float | None, float | None] | None = None
            for key in keys:
                uri = opts.get(key, {}).get("uri", "")
                if not uri:
                    continue
                if trim is None:
                    trim = self.offering_trim(name)
                yield Playout(
                    offering=name,
                    key=key,
                    manifest_url=(f"{self.fabric_node}/q/{self.qhash}"
                                  f"/rep/playout/{name}/{uri}"),
                    entry_s=trim[0],
                    exit_s=trim[1],
                )

    def _rank_playouts(
        self,
        candidates: list[str],
        keys: tuple[str, ...],
        key_major: bool = False,
    ) -> list[Playout]:
        """Usable playouts, best first.

        Untrimmed offerings rank before exit-only trims, which rank before
        offerings with an entry point: a correct timeline beats a faster or
        clearer playout. Within a trim class the order is offering order then
        key order, or key order then offering order when ``key_major``.
        """
        keys = tuple(keys)

        def trim_rank(playout: Playout) -> int:
            if playout.trimmed:
                return 2
            return 1 if playout.exit_s is not None else 0

        def rank(playout: Playout) -> tuple[int, int, int]:
            key_idx = keys.index(playout.key)
            off_idx = candidates.index(playout.offering)
            if key_major:
                return trim_rank(playout), key_idx, off_idx
            return trim_rank(playout), off_idx, key_idx

        return sorted(self._iter_playouts(candidates, keys), key=rank)

    @staticmethod
    def _trimmed_message(ranked: list[Playout]) -> str:
        return (
            "Every usable offering is trimmed, so its playout timeline would "
            "be offset from the Tagstore tracks: "
            + "; ".join(p.describe() for p in ranked)
            + ". Refusing to download it; pass allow_trimmed=True to override "
            "(clips cut at Tagstore times will then need the offset applied)."
        )

    def _resolve_playout(
        self,
        offering: str | None = None,
        clear_only: bool = False,
        keys: tuple[str, ...] | None = None,
        require_untrimmed: bool = False,
    ) -> tuple[str, str, str]:
        """Resolve (offering, playout_key, manifest_url) in one pass.

        Fetches options.json once per probed offering. Offerings without an
        entry point are preferred (see _rank_playouts).

        Args:
            offering: Restrict to this offering (auto-discovered if None).
            clear_only: Only accept clear (non-DRM) playouts, as media/files
                requires.
            keys: Explicit playout keys to accept, overriding clear_only.
            require_untrimmed: Raise TrimmedOfferingError instead of falling
                back to an offering whose playout starts at an entry point.
        """
        if keys is None:
            keys = self._CLEAR_PLAYOUT_KEYS if clear_only else self._PLAYOUT_KEYS
        candidates = [offering] if offering else self.available_offerings()
        if not candidates:
            raise ValueError("No playout offerings found in content metadata")

        ranked = self._rank_playouts(candidates, keys)
        if not ranked:
            raise ValueError(
                f"No offering with a supported playout found "
                f"(keys={keys}). Probed: {candidates}"
            )
        best = ranked[0]
        if best.trimmed:
            if require_untrimmed:
                raise TrimmedOfferingError(self._trimmed_message(ranked))
            logger.warning(
                "Using trimmed offering %s: its playout timeline does not "
                "start at mezzanine 0", best.describe(),
            )
        return best.offering, best.key, best.manifest_url

    def resolve_offering(
        self, clear_only: bool = False, require_untrimmed: bool = False,
    ) -> str:
        """Find the best offering that has a usable playout option.

        Offerings without an entry point win over trimmed ones; otherwise the
        first offering in metadata order.

        Args:
            clear_only: Only consider offerings with a clear (non-DRM)
                playout (dash-clear, hls-clear), as media/files requires.
            require_untrimmed: Raise TrimmedOfferingError when only trimmed
                offerings qualify.
        """
        return self._resolve_playout(
            clear_only=clear_only, require_untrimmed=require_untrimmed,
        )[0]

    def total_duration_ms(self, offering: str | None = None) -> int:
        """Return the total content duration in milliseconds from a playout manifest."""
        _, playout_key, manifest_url = self._resolve_playout(offering)
        resp = requests.get(manifest_url)
        resp.raise_for_status()

        if playout_key.startswith("dash"):
            return self._parse_dash_duration(resp.text)
        return self._parse_hls_duration(resp.text, manifest_url)

    def _parse_dash_duration(self, mpd_text: str) -> int:
        match = re.search(
            r'mediaPresentationDuration="PT(?:(\d+)H)?(?:(\d+)M)?(?:([\d.]+)S)?"',
            mpd_text,
        )
        if not match:
            raise ValueError(
                "Could not find mediaPresentationDuration in DASH manifest")

        hours = float(match.group(1) or 0)
        minutes = float(match.group(2) or 0)
        seconds = float(match.group(3) or 0)
        return int((hours * 3600 + minutes * 60 + seconds) * 1000)

    def _parse_hls_duration(self, master_text: str, master_url: str) -> int:
        """Extract total duration from an HLS playlist.

        If the master playlist is a multivariant playlist, fetches the first
        media playlist and sums its #EXTINF durations.  If it already contains
        #EXTINF lines (single-stream playlist), sums them directly.
        """
        def extinf_total_ms(text: str) -> int | None:
            values = re.findall(r"#EXTINF:([\d.]+)", text)
            if not values:
                return None
            return int(sum(float(v) for v in values) * 1000)

        if (total := extinf_total_ms(master_text)) is not None:
            return total

        # Otherwise, find the first media playlist URI from the master
        for line in master_text.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                media_resp = requests.get(urljoin(master_url, line))
                media_resp.raise_for_status()
                if (total := extinf_total_ms(media_resp.text)) is not None:
                    return total
                break

        raise ValueError("Could not determine duration from HLS playlist")

    def playout_options(self, offering: str | None = None) -> Dict[str, Any]:
        """Fetch playout options for the given offering.

        Returns the raw options dict from:
            GET /q/{qhash}/rep/playout/{offering}/options.json
        """
        if offering is None:
            offering = self.resolve_offering()
        url = f"{self.fabric_node}/q/{self.qhash}/rep/playout/{offering}/options.json"
        resp = requests.get(url, params={"authorization": self._token})
        resp.raise_for_status()
        return resp.json()

    def resolve_audio_playlist(
        self, offering: str | None = None, allow_trimmed: bool = False,
    ) -> tuple[Playout, str]:
        """The audio-only HLS media playlist to download, and its playout.

        HLS serves each audio representation as its own audio-only playlist,
        the only audio-only route: media/files always transcodes a video rep.

        Offerings whose playout starts at mezzanine 0 win, whatever their
        format; among those a clear playout beats hls-aes128. When only
        trimmed offerings qualify, TrimmedOfferingError is raised unless
        ``allow_trimmed``, and the caller then owns the offset
        (Playout.entry_s).

        The URL embeds a short-lived session token from options.json: resolve
        it per session, never cache it.
        """
        candidates = [offering] if offering else self.available_offerings()
        if not candidates:
            raise ValueError("No playout offerings found in content metadata")
        ranked = self._rank_playouts(
            candidates, self._FFMPEG_HLS_KEYS, key_major=True)
        if not ranked:
            raise ValueError(
                f"No offering with an ffmpeg-readable HLS playout "
                f"(keys={self._FFMPEG_HLS_KEYS}). Probed: {candidates}")
        best = ranked[0]
        if best.trimmed:
            if not allow_trimmed:
                raise TrimmedOfferingError(self._trimmed_message(ranked))
            logger.warning(
                "Downloading trimmed offering %s: the file's timeline starts "
                "at its entry point, not at mezzanine 0", best.describe(),
            )
        resp = requests.get(best.manifest_url)
        resp.raise_for_status()

        audio_uris = self._hls_audio_uris(resp.text)
        if not audio_uris:
            raise ValueError("No audio playlists found in HLS master playlist")
        return best, urljoin(best.manifest_url, self._prefer_audio(audio_uris))

    def audio_playlist_url(
        self, offering: str | None = None, allow_trimmed: bool = False,
    ) -> str:
        """Absolute URL of the preferred audio-only HLS playlist.

        See resolve_audio_playlist for the offering preference and the
        TrimmedOfferingError it raises.
        """
        return self.resolve_audio_playlist(offering, allow_trimmed=allow_trimmed)[1]

    def default_representations(
        self, offering: str | None = None,
    ) -> tuple[list[str], str]:
        """Return (video_rep_ids, audio_rep_id) for a download.

        Parses the playout manifest (DASH or HLS) to get representation IDs
        (e.g. 'videovideo_640x360_h264@1055556', 'audio_1audio_aac@128000').
        Returns video reps sorted by bandwidth (ascending) so callers can
        fall back to the next rep if the lowest one is rejected by the API.
        Prefers english_5_1 > english_stereo for audio.
        """
        _, playout_key, manifest_url = self._resolve_playout(offering)
        resp = requests.get(manifest_url)
        resp.raise_for_status()

        if playout_key.startswith("dash"):
            return self._parse_dash_representations(resp.text)
        return self._parse_hls_representations(resp.text)

    def _parse_dash_representations(
        self, mpd: str,
    ) -> tuple[list[str], str]:
        video_reps: list[tuple[str, int]] = []
        audio_reps: list[tuple[str, int]] = []

        for adapt_match in re.finditer(
            r'<AdaptationSet[^>]*contentType="(\w+)"[^>]*>(.*?)</AdaptationSet>',
            mpd, re.DOTALL,
        ):
            content_type = adapt_match.group(1)
            block = adapt_match.group(2)
            for rep_match in re.finditer(
                r'<Representation[^>]*\bbandwidth="(\d+)"[^>]*\bid="([^"]+)"',
                block,
            ):
                bw = int(rep_match.group(1))
                rep_id = rep_match.group(2)
                if content_type == "video":
                    video_reps.append((rep_id, bw))
                elif content_type == "audio":
                    audio_reps.append((rep_id, bw))

        return self._select_reps(video_reps, audio_reps)

    def _parse_hls_representations(
        self, master: str,
    ) -> tuple[list[str], str]:
        video_reps: list[tuple[str, int]] = []

        lines = master.splitlines()
        for i, line in enumerate(lines):
            # Video: #EXT-X-STREAM-INF followed by URI line
            m = re.match(r"#EXT-X-STREAM-INF:.*BANDWIDTH=(\d+)", line)
            if m and i + 1 < len(lines):
                bw = int(m.group(1))
                uri_line = lines[i + 1].strip()
                # URI pattern: video/{rep_id}/playlist.m3u8?...
                rep_m = re.match(r"(?:video/)?([^/]+)/playlist\.m3u8", uri_line)
                if rep_m:
                    video_reps.append((rep_m.group(1), bw))

        # Audio rep id is the last path component before playlist.m3u8
        audio_reps: list[tuple[str, int]] = []
        for uri in self._hls_audio_uris(master):
            rep_m = re.search(r"([^/]+)/playlist\.m3u8", uri)
            if rep_m:
                audio_reps.append((rep_m.group(1), 0))

        return self._select_reps(video_reps, audio_reps)

    @staticmethod
    def _hls_audio_uris(master: str) -> list[str]:
        """URIs of #EXT-X-MEDIA TYPE=AUDIO entries, relative to the master."""
        uris: list[str] = []
        for line in master.splitlines():
            if line.startswith("#EXT-X-MEDIA:") and "TYPE=AUDIO" in line:
                m = re.search(r'URI="([^"]+)"', line)
                if m:
                    uris.append(m.group(1))
        return uris

    @classmethod
    def _prefer_audio(cls, names: list[str]) -> str:
        """First entry matching _AUDIO_PREFS, else the first entry."""
        for pref in cls._AUDIO_PREFS:
            for name in names:
                if pref in name:
                    return name
        return names[0]

    @classmethod
    def _select_reps(
        cls,
        video_reps: list[tuple[str, int]],
        audio_reps: list[tuple[str, int]],
    ) -> tuple[list[str], str]:
        """Pick ordered video reps and preferred audio rep."""
        if not video_reps:
            raise ValueError(
                "No video representations found in playout manifest")
        if not audio_reps:
            raise ValueError(
                "No audio representations found in playout manifest")

        video_sorted = [r[0] for r in sorted(video_reps, key=lambda x: x[1])]
        audio_rep = cls._prefer_audio([r[0] for r in audio_reps])
        return video_sorted, audio_rep

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
