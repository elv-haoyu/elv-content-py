#!/usr/bin/env python3
"""Download the raw fabric media parts of a content object.

Media is stored as an ordered list of encrypted parts per stream, so downloading
means fetching each part in order. Parts are pulled through the `elv` CLI because
decryption happens client-side -- a plain HTTP GET returns bytes that look fine
but are not media.

By default this fetches one video stream and one English audio stream, preferring
5.1 over stereo, and extracts the center channel from a 5.1 track (the center
channel carries dialogue, so it is the useful one for speech work). Pass
`all_streams` to take every stream instead.

Layout under <output_root>/<qid>/:

    manifest.json                            streams, part hashes, durations
    video/0000_hqpe....mp4
    english_5_1__.../0000_hqpe....m4a        as downloaded (5.1)
    english_5_1__..._center/0000_hqpe....wav mono center channel

Re-running skips parts already on disk, so an interrupted download resumes.

This is the whole-object counterpart to `downloader.ContentDownloader`, which
transcodes time ranges through the media/files API instead; use that one for
clips, or when the parts cannot be decrypted locally.

Auth is the signing key -- `--secret 0x<hex>`, else $ELV_SECRETS -- else an
already-minted token: `--token`, or the last line of ./token.txt. A bare token
cannot decrypt the parts of encrypted content.

Usage:
    ELV_SECRETS=0x... python parts.py iq__4Dzv28AJhNGsK2FkUEf7FyxYcRwH
    python parts.py <qid> --secret 0x<hex> --all-streams
    python parts.py <qid> --token token.txt --language es
"""

import json
import logging
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from fractions import Fraction
from pathlib import Path

import requests

try:
    from .config import fabric_nodes, resolve_config_url
    from .elv_token import (auth_flags, create_token, elv_binary, find_secret,
                            resolve_token, run_elv)
    from .media import extract_center, ffmpeg_binary, looks_like_mp4
except ImportError:  # running this file directly rather than as a package module
    from config import fabric_nodes, resolve_config_url
    from elv_token import (auth_flags, create_token, elv_binary, find_secret,
                           resolve_token, run_elv)
    from media import extract_center, ffmpeg_binary, looks_like_mp4


logger = logging.getLogger(__name__)

WORKERS = 1
EXTRACT_WORKERS = 4
TIMEOUT = 300
RETRIES = 4
EXTENSIONS = {"video": ".mp4", "audio": ".m4a"}


# ---------------------------------------------------------------------------
# Stream metadata -> download plan
# ---------------------------------------------------------------------------

def part_duration(stream_meta: dict, sources: list) -> float:
    first = sources[0]
    if isinstance(first, dict):
        return float(first["duration"]["float"])
    # Array form: [part hash, duration in ticks] scaled by the stream time base.
    return int(first[1]) * float(Fraction(stream_meta["duration"]["time_base"]))


def parse_fps(rate) -> float | None:
    if rate is None:
        return None
    if "/" in str(rate):
        numerator, denominator = str(rate).split("/")
        return float(numerator) / float(denominator)
    return float(rate)


def stream_details(name: str, stream: dict, transcodes: dict | None) -> dict | None:
    """Resolve a playout stream to the metadata block holding its parts.

    Modern objects point at an entry in `transcodes`; legacy objects (no
    `transcodes` subtree) carry codec, rate and sources inline.
    """
    if transcodes is None:
        return stream if stream.get("sources") else None

    ids = {
        representation["transcode_id"]
        for representation in stream.get("representations", {}).values()
        if "transcode_id" in representation
    }
    if not ids:
        # Thumbnail and subtitle streams carry no transcode, so no parts.
        return None
    if len(ids) > 1:
        logger.warning("%s: multiple transcode ids %s, using first", name, sorted(ids))
    return transcodes[sorted(ids)[0]]["stream"]


def build_plan(streams: dict, transcodes: dict | None) -> list[dict]:
    """One entry per downloadable stream, in fabric part order."""
    plan = []
    for name, stream in sorted(streams.items()):
        meta = stream_details(name, stream, transcodes)
        if meta is None:
            continue
        if meta.get("codec_type") not in EXTENSIONS:
            # Caption/subtitle streams have parts but are not media we download.
            continue
        sources = meta["sources"]
        plan.append(
            {
                "stream": name,
                "codec_type": meta["codec_type"],
                "language": meta.get("language") or "",
                "channels": meta.get("channels"),
                "channel_layout": meta.get("channel_layout") or "",
                "label": meta.get("label") or "",
                "part_duration": part_duration(meta, sources),
                "fps": parse_fps(meta.get("rate")) if meta["codec_type"] == "video" else None,
                "parts": [
                    source["source"] if isinstance(source, dict) else source[0]
                    for source in sources
                ],
            }
        )
    return plan


def has_center_channel(entry: dict) -> bool:
    """5.1 and friends put dialogue in a discrete front-center channel; stereo has
    no such channel to extract."""
    layout = entry["channel_layout"].lower()
    if "5.1" in layout or "7.1" in layout:
        return True
    return bool(entry["channels"]) and entry["channels"] >= 6


def select_streams(plan: list[dict], language: str = "en") -> list[dict]:
    """One video stream plus one audio stream in `language`, 5.1 preferred."""
    selected = []

    videos = [entry for entry in plan if entry["codec_type"] == "video"]
    if videos:
        # A stream literally named "video" is the main one when present.
        exact = [entry for entry in videos if entry["stream"] == "video"]
        selected.append(exact[0] if exact else videos[0])

    audios = [entry for entry in plan if entry["codec_type"] == "audio"]
    wanted = [
        entry for entry in audios
        if entry["language"].lower().startswith(language.lower())
    ]
    if not wanted and audios:
        logger.info("no %r audio stream; falling back to all audio streams", language)
        wanted = audios
    if wanted:
        # Prefer a center-carrying layout, then the most channels.
        wanted.sort(key=lambda e: (has_center_channel(e), e["channels"] or 0), reverse=True)
        selected.append(wanted[0])

    return selected


# ---------------------------------------------------------------------------
# Downloader
# ---------------------------------------------------------------------------

class PartDownloader:
    """Fetch a content object's media parts through the `elv` CLI.

    Args:
        secret: hex signing key; mints its own tokens and can decrypt parts.
        token: an existing auth token, when no signing key is available. Parts
            of encrypted content cannot be decrypted with this alone.
        config_url: fabric config url (default: $ELV_CONFIG_URL, else main net).
        nodes: fabric nodes for metadata reads (default: from the config url).
        space: qspace name used in metadata URLs.
    """

    def __init__(self, secret: str | None = None, token: str | None = None,
                 config_url: str | None = None, nodes=None, space: str = "main",
                 workers: int = WORKERS, extract_workers: int = EXTRACT_WORKERS):
        if not secret and not token:
            raise ValueError("PartDownloader needs either a secret or a token")
        self._secret = secret
        self._token = token
        self.config_url = resolve_config_url(config_url)
        self._nodes = tuple(nodes) if nodes else None
        self.space = space
        self.workers = workers
        self.extract_workers = extract_workers
        self._libraries: dict[str, str | None] = {}

    # --- fabric plumbing ---

    @property
    def nodes(self) -> tuple[str, ...]:
        if self._nodes is None:
            self._nodes = fabric_nodes(self.config_url)
        return self._nodes

    def library_id(self, qid: str) -> str | None:
        """`elv content library` answers for any object, including ones absent
        from a local catalog. Its output is {"<qid>": "<library id>"}."""
        if not self._secret:
            # A blockchain read, which an externally provided token cannot sign.
            return None
        if qid not in self._libraries:
            arguments = ["content", "library", qid, *auth_flags(self._secret, self._token)]
            try:
                self._libraries[qid] = run_elv(arguments, self.config_url).get(qid)
            except (RuntimeError, subprocess.SubprocessError) as error:
                logger.warning("%s: could not resolve library id — %s", qid, error)
                self._libraries[qid] = None
        return self._libraries[qid]

    def auth_token(self, qid: str) -> str:
        """A token authorized for `qid`. Group memberships are resolved by the
        CLI and are what non-public metadata reads are checked against."""
        if self._token:
            return self._token
        return create_token(
            self._secret, qid, library_id=self.library_id(qid), config_url=self.config_url
        )

    def fetch_metadata(self, qid: str, token: str | None = None) -> tuple[dict, dict | None]:
        """Return (streams, transcodes). `transcodes` is None for legacy objects,
        whose stream details live inline in media_struct instead."""
        headers = {"Authorization": f"Bearer {token or self.auth_token(qid)}"}
        params = {"resolve_links": "true"}
        errors = []

        for node in self.nodes:
            base = f"{node}/s/{self.space}/q/{qid}"

            def get(path):
                response = requests.get(
                    f"{base}/{path}", headers=headers, params=params, timeout=180
                )
                response.raise_for_status()
                return response.json()

            try:
                return get("meta/offerings/default/playout/streams"), get("meta/transcodes")
            except requests.HTTPError as error:
                if error.response is None or error.response.status_code != 404:
                    errors.append(f"{node}: {error}")
                    continue
            except requests.RequestException as error:
                errors.append(f"{node}: {error}")
                continue

            # Legacy layout: codec, rate and parts sit on the stream itself.
            logger.info("%s: no transcodes metadata; using legacy media_struct layout", qid)
            try:
                return get("meta/offerings/default/media_struct/streams"), None
            except requests.RequestException as error:
                errors.append(f"{node}: {error}")

        raise RuntimeError(f"could not read stream metadata for {qid}: {'; '.join(errors)}")

    # --- planning ---

    def plan(self, qid: str, language: str = "en", all_streams: bool = False,
             streams: list[str] | None = None) -> list[dict]:
        """The streams to download, after selection."""
        stream_meta, transcodes = self.fetch_metadata(qid)
        plan = build_plan(stream_meta, transcodes)
        if not plan:
            return []
        if streams:
            wanted = set(streams)
            return [entry for entry in plan if entry["stream"] in wanted]
        if all_streams:
            return plan
        return select_streams(plan, language)

    # --- part reads ---

    def download_part(self, qid: str, part_hash: str, destination: Path,
                      library_id: str | None = None) -> str:
        """Fetch one part. Returns "cached", "downloaded" or "failed"."""
        if destination.exists() and destination.stat().st_size > 0:
            if looks_like_mp4(destination):
                return "cached"
            destination.unlink()  # truncated or undecrypted from an earlier run

        command = [
            elv_binary(), "content", "part", "read", qid, part_hash, str(destination),
            "--decryption-mode", "decrypt",
            *auth_flags(self._secret, self._token),
            "--config-url", self.config_url,
        ]
        if library_id:
            command += ["--library", library_id]

        last_error = None
        for attempt in range(RETRIES):
            result = subprocess.run(command, capture_output=True, text=True, timeout=TIMEOUT)
            if result.returncode == 0 and looks_like_mp4(destination):
                return "downloaded"
            last_error = (result.stderr or result.stdout).strip()[:200] or "not valid media"
            if destination.exists():
                destination.unlink()
            time.sleep(2 ** attempt)
        logger.error("FAILED %s: %s", part_hash, last_error)
        return "failed"

    # --- the whole object ---

    def download(self, qid: str, output_root="content", language: str = "en",
                 all_streams: bool = False, streams: list[str] | None = None,
                 max_parts: int | None = None, center: bool = True,
                 center_rate: int | None = None, workers: int | None = None) -> dict:
        """Download the selected streams into <output_root>/<qid>, write a manifest
        and return it. Only what is missing on disk is fetched.
        """
        library_id = self.library_id(qid)
        logger.info("%s  lib=%s", qid, library_id)

        plan = self.plan(qid, language, all_streams, streams)
        if not plan:
            raise RuntimeError(f"no downloadable streams found for {qid}")

        destination_root = Path(output_root) / qid
        destination_root.mkdir(parents=True, exist_ok=True)

        jobs = []
        for entry in plan:
            parts = entry["parts"][:max_parts] if max_parts else entry["parts"]
            entry["selected_parts"] = parts
            extension = EXTENSIONS.get(entry["codec_type"], ".bin")
            stream_dir = destination_root / entry["stream"]
            stream_dir.mkdir(parents=True, exist_ok=True)
            entry["dir"] = stream_dir
            entry["files"] = [
                stream_dir / f"{index:04d}_{part_hash}{extension}"
                for index, part_hash in enumerate(parts)
            ]
            detail = entry["label"] or entry["channel_layout"] or ""
            logger.info(
                "  %-40s %-5s %-22s %4d parts  %6.1f min",
                entry["stream"], entry["codec_type"], detail,
                len(parts), len(parts) * entry["part_duration"] / 60,
            )
            jobs += list(zip(entry["files"], parts))

        logger.info("downloading %d parts into %s ...", len(jobs), destination_root)
        started = time.time()
        counts = {"downloaded": 0, "cached": 0, "failed": 0}
        with ThreadPoolExecutor(max_workers=workers or self.workers) as pool:
            futures = [
                pool.submit(self.download_part, qid, part_hash, path, library_id)
                for path, part_hash in jobs
            ]
            for done, future in enumerate(futures, 1):
                counts[future.result()] += 1
                if done % 100 == 0 or done == len(futures):
                    logger.info("  %d/%d parts  %.0fs", done, len(futures),
                                time.time() - started)

        center_counts = self._extract_centers(plan, destination_root, center, center_rate)

        manifest = {
            "qid": qid,
            "library_id": library_id,
            "config_url": self.config_url,
            "counts": {"parts": counts, "center": center_counts},
            "streams": [
                {
                    "stream": entry["stream"],
                    "codec_type": entry["codec_type"],
                    "language": entry["language"],
                    "label": entry["label"],
                    "channels": entry["channels"],
                    "channel_layout": entry["channel_layout"],
                    "part_duration": entry["part_duration"],
                    "fps": entry["fps"],
                    "num_parts": len(entry["selected_parts"]),
                    "dir": entry["dir"].name,
                    "center_dir": entry["center_dir"].name if entry["center_dir"] else None,
                    "parts": entry["selected_parts"],
                }
                for entry in plan
            ],
        }
        (destination_root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

        size = sum(path.stat().st_size for path in destination_root.rglob("*") if path.is_file())
        logger.info(
            "parts: downloaded=%d cached=%d failed=%d — %.2f GiB in %s",
            counts["downloaded"], counts["cached"], counts["failed"],
            size / 2 ** 30, destination_root,
        )
        if any(center_counts.values()):
            logger.info(
                "center: extracted=%d cached=%d failed=%d",
                center_counts["extracted"], center_counts["cached"], center_counts["failed"],
            )
        return manifest

    def _extract_centers(self, plan: list[dict], destination_root: Path, center: bool,
                         center_rate: int | None) -> dict:
        counts = {"extracted": 0, "cached": 0, "failed": 0}
        for entry in plan:
            entry["center_dir"] = None
            if not center or entry["codec_type"] != "audio":
                continue
            if not has_center_channel(entry):
                logger.info("%s: %s has no center channel, skipping extraction",
                            entry["stream"], entry["channel_layout"] or "unknown layout")
                continue
            center_dir = destination_root / f"{entry['stream']}_center"
            center_dir.mkdir(parents=True, exist_ok=True)
            entry["center_dir"] = center_dir
            sources = [path for path in entry["files"] if path.exists()]
            logger.info("extracting center channel from %s (%d parts) -> %s",
                        entry["stream"], len(sources), center_dir.name)
            ffmpeg = ffmpeg_binary()
            with ThreadPoolExecutor(max_workers=self.extract_workers) as pool:
                futures = [
                    pool.submit(extract_center, path, center_dir / f"{path.stem}.wav",
                                center_rate, ffmpeg)
                    for path in sources
                ]
                for future in futures:
                    counts[future.result()] += 1
        return counts


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def add_arguments(parser):
    parser.add_argument("qid", help="content object id (iq__...)")
    parser.add_argument("--secret", help="hex signing key (default: $ELV_SECRETS)")
    parser.add_argument("--token",
                        help="auth token, or a file whose last line is one, instead "
                             "of a signing key (default: ./token.txt)")
    parser.add_argument("--config-url", help="fabric config url")
    parser.add_argument("-o", "--output-root", default="content",
                        help="parts land in <output-root>/<qid> (default: content)")
    parser.add_argument("--language", default="en", help="audio language prefix (default: en)")
    parser.add_argument("--all-streams", action="store_true",
                        help="download every stream instead of one video + one audio")
    parser.add_argument("--streams", help="comma-separated stream names, overrides selection")
    parser.add_argument("--no-center", action="store_true",
                        help="skip center-channel extraction")
    parser.add_argument("--center-rate", type=int,
                        help="resample extracted center audio to this rate (default: source rate)")
    parser.add_argument("--max-parts", type=int,
                        help="stop after N parts per stream (smoke test)")
    parser.add_argument("--workers", type=int, default=WORKERS)
    return parser


def run(args) -> int:
    secret, token = find_secret(args.secret), None
    if not secret:
        try:
            token = resolve_token(args.token)
        except (FileNotFoundError, ValueError) as error:
            raise SystemExit(f"no auth; pass --secret or --token ({error})")

    downloader = PartDownloader(
        secret=secret, token=token, config_url=args.config_url, workers=args.workers
    )
    manifest = downloader.download(
        args.qid,
        output_root=args.output_root,
        language=args.language,
        all_streams=args.all_streams,
        streams=args.streams.split(",") if args.streams else None,
        max_parts=args.max_parts,
        center=not args.no_center,
        center_rate=args.center_rate,
    )
    failed = manifest["counts"]["parts"]["failed"] + manifest["counts"]["center"]["failed"]
    return 1 if failed else 0


def main():
    import argparse
    import sys

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sys.exit(run(add_arguments(parser).parse_args()))


if __name__ == "__main__":
    main()
