from .content import Content, DEFAULT_CONFIG_URL, TrimmedOfferingError
from .elv_token import find_secret
from .media import ffmpeg_binary, looks_like_mp4
from .parts import PartDownloader, build_plan, select_streams
import json
import logging
import subprocess
import time
from datetime import datetime, timezone

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

POLL_INTERVAL = 5        # seconds between job status checks
MAX_POLL_ATTEMPTS = 120  # x POLL_INTERVAL: give up on a job after 10 min
CHUNK_SIZE = 1 << 20     # bytes per streamed read
CHUNK_WORKERS = 4        # concurrent transcode jobs when chunking a title
PROGRESS_INTERVAL = 5    # seconds between download progress lines
MB = 1024 * 1024

AUDIO_TRACK_FILENAME = "audio_full.mp4"
# Sidecar next to audio_full.mp4: the offering and playout it came from and
# where its timeline starts, so a cached track can be judged without
# re-probing the fabric.
AUDIO_TRACK_SIDECAR = "audio_full.json"
# Sidecar listing the mezzanine audio parts (``0000_<hash>.m4a``, ...) in
# stream order. Part i starts at i x part_duration: the Tagstore timeline.
PARTS_SIDECAR = "parts.json"
PARTS_WORKERS = 8
FFMPEG_TIMEOUT = 3600    # seconds, whole-title HLS audio download
# Matches the {index:05d}_{start}-{end}.mp4 names _download_parts_chunked
# writes; keep the two in step.
CHUNK_GLOB = "[0-9]*_*.mp4"


def _stream_to_file(response, path: Path, label: str) -> int:
    """Write a streamed response body to `path`, logging progress as it goes.

    Returns the number of bytes written. `Content-Length` is absent on chunked
    responses, in which case only the running total is logged.
    """
    total = int(response.headers.get("Content-Length") or 0)
    logger.info("[%s] downloading %s", label,
                f"{total / MB:.1f} MB" if total else "unknown size")

    written = 0
    started = last = time.monotonic()
    with open(path, "wb") as f:
        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            f.write(chunk)
            written += len(chunk)
            now = time.monotonic()
            if now - last >= PROGRESS_INTERVAL:
                last = now
                rate = written / MB / max(now - started, 1e-6)
                if total:
                    logger.info("[%s] %.1f/%.1f MB (%d%%) %.1f MB/s", label,
                                written / MB, total / MB,
                                written * 100 // total, rate)
                else:
                    logger.info("[%s] %.1f MB %.1f MB/s", label,
                                written / MB, rate)

    elapsed = time.monotonic() - started
    logger.info("[%s] done: %.2f MB in %.0fs", label, written / MB, elapsed)
    return written


def write_audio_track_provenance(out_dir: Path, provenance: dict) -> Path:
    """Record where ``audio_full.mp4`` came from, next to it."""
    path = Path(out_dir) / AUDIO_TRACK_SIDECAR
    path.write_text(json.dumps(provenance, indent=2) + "\n")
    return path


def read_audio_track_provenance(out_dir: Path) -> dict | None:
    """The sidecar written by download_audio_track, or None when absent.

    A track without one predates the sidecar, so nothing is known about the
    offering it came from or whether its timeline starts at mezzanine 0.
    """
    path = Path(out_dir) / AUDIO_TRACK_SIDECAR
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def read_parts_provenance(out_dir: Path) -> dict | None:
    """The sidecar written by download_mezzanine_parts, or None when absent."""
    path = Path(out_dir) / PARTS_SIDECAR
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) and data.get("parts") else None


def cached_mezzanine_parts(out_dir: Path) -> list[Path] | None:
    """The parts listed in ``parts.json`` when every one is on disk and looks
    like media; None otherwise, so the caller fetches (or re-fetches)."""
    provenance = read_parts_provenance(out_dir)
    if provenance is None:
        return None
    paths = [Path(out_dir) / name for name in provenance["parts"]]
    if all(p.exists() and p.stat().st_size > 0 and looks_like_mp4(p) for p in paths):
        return paths
    return None


def warn_cached_track(track_path: Path) -> None:
    """Say what is known about a cached track's timeline before reusing it."""
    provenance = read_audio_track_provenance(Path(track_path).parent)
    if provenance is None:
        logger.warning(
            "Reusing cached %s that has no %s: the offering it came from "
            "and its timeline offset are unknown. Delete it to re-download "
            "from an untrimmed offering.",
            track_path, AUDIO_TRACK_SIDECAR,
        )
    elif provenance.get("timeline") != "mezzanine":
        logger.warning(
            "Reusing cached %s downloaded from trimmed offering %s "
            "(entry_point=%s s): clips cut at Tagstore times are offset.",
            track_path, provenance.get("offering"),
            provenance.get("entry_point_s"),
        )


class ContentDownloader:
    """Download clips and whole-title audio from the Eluvio content fabric."""

    def __init__(self, auth_token: str, config_url: str = DEFAULT_CONFIG_URL):
        self._token = auth_token
        self._config_url = config_url
        self._content_cache: dict[str, Content] = {}

    def _get_content(self, qhit: str) -> Content:
        if qhit not in self._content_cache:
            self._content_cache[qhit] = Content(
                qhit, self._token, self._config_url)
        return self._content_cache[qhit]

    def download(
        self,
        content_id: str,
        start_ms: int,
        end_ms: int,
        output_dir: str = "downloads",
        offering: str | None = None,
        format: str = "mp4",
        audio_only: bool = False,
        representation: Optional[str] = None,
    ) -> Optional[Path]:
        """Download a media segment from the content fabric.

        Args:
            content_id: Object ID (iq__...) or version hash (hq__...).
            start_ms: Clip start time in milliseconds.
            end_ms: Clip end time in milliseconds.
            output_dir: Directory to save the downloaded file.
            offering: Playout offering (auto-discovered if None).
            format: Container format (default: "mp4").
            audio_only: If True, omit the video representation from the
                request so only the audio track is transcoded.
            representation: Which video rendition to transcode -- an exact id
                from `representations()`, or "lowest"/"highest". The default
                tries every rendition, lowest bandwidth first. The fabric
                generates these on playout, so a lower one is genuinely
                smaller than what is stored.

        Returns:
            Path to the downloaded file, or None on failure.
        """
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{content_id}_{start_ms}-{end_ms}.{format}"
        output_path = out_dir / filename

        if output_path.exists():
            return output_path

        content = self._get_content(content_id)

        # One offering for every call below. media/files cannot transcode
        # DRM content, so it must be a clear one (dash-clear or hls-clear).
        if offering is None:
            offering = content.resolve_offering(clear_only=True)

        video_reps, audio_rep = content.default_representations(offering)
        logger.debug("Available video reps (by bandwidth): %s  audio=%s",
                     video_reps, audio_rep)
        if representation == "lowest":
            video_reps = video_reps[:1]
        elif representation == "highest":
            video_reps = video_reps[-1:]
        elif representation:
            if representation not in video_reps:
                logger.warning("%s is not one of %s", representation, video_reps)
            video_reps = [representation]
        if representation and not audio_only:
            logger.info("[%s] representation %s", filename, video_reps[0])

        start_s = f"{start_ms / 1000:.4f}s"
        end_s = f"{end_ms / 1000:.4f}s"

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        params = {"authorization": self._token}
        nodes = content._client.fabric_uris

        # audio_only sends no video rep at all: short clips otherwise fail in
        # the video transcoder (translateToMuxSpec).
        if audio_only:
            video_reps = [None]

        # Video reps lowest bandwidth first; a 400 moves on to the next one.
        for rep_idx, video_rep in enumerate(video_reps):
            # Only fields the API knows: it rejects any other outright.
            body = {
                "format": format,
                "offering": offering,
                "start_ms": start_s,
                "end_ms": end_s,
                "audio": audio_rep,
            }
            if video_rep is not None:
                body["representation"] = video_rep

            logger.debug("Trying rep %d/%d (%s) for %s  (qhash=%s, nodes=%d)",
                         rep_idx + 1, len(video_reps),
                         video_rep or "audio-only",
                         filename, content.qhash, len(nodes))

            rep_rejected = False
            node_errors: list[dict] = []

            for i, node in enumerate(nodes, 1):
                base_url = f"{node}/q/{content.qhash}/call/media/files"

                # --- Step 1: Create transcode job ---
                logger.debug("[%s node %d/%d] POST %s  body=%s",
                             filename, i, len(nodes), base_url, body)
                try:
                    resp = requests.post(
                        base_url, json=body, headers=headers, params=params
                    )
                except requests.RequestException as exc:
                    reason = f"connection error on job create: {exc}"
                    logger.debug("[%s node %d/%d] %s — %s — trying next",
                                 filename, i, len(nodes), node, reason)
                    node_errors.append({"node": node, "step": "create",
                                        "reason": reason})
                    continue

                if not resp.ok:
                    reason = (f"HTTP {resp.status_code} on job create: "
                              f"{resp.text[:500]}")
                    logger.debug(
                        "[%s node %d/%d] %s — %s — trying next",
                        filename, i, len(nodes), node, reason,
                    )
                    node_errors.append({"node": node, "step": "create",
                                        "reason": reason})
                    # 400: the rep itself is invalid on every node.
                    if resp.status_code == 400:
                        rep_rejected = True
                        break
                    continue
                job_data = resp.json()
                job_id = job_data.get("job_id")
                if not job_id:
                    reason = f"no job_id in response: {job_data}"
                    logger.debug(
                        "[%s node %d/%d] %s — %s",
                        filename, i, len(nodes), node, reason)
                    node_errors.append({"node": node, "step": "create",
                                        "reason": reason})
                    continue

                logger.debug("[%s node %d/%d] Job created: %s on %s",
                             filename, i, len(nodes), job_id, node)

                # --- Step 2: Poll until completed ---
                status_url = f"{base_url}/{job_id}"
                status = "processing"
                last_status_data = {}
                job_started = time.monotonic()
                logger.info("[%s] transcoding, polling every %ds", filename,
                            POLL_INTERVAL)
                for attempt in range(MAX_POLL_ATTEMPTS):
                    if attempt:
                        time.sleep(POLL_INTERVAL)
                    try:
                        status_resp = requests.get(status_url, params=params)
                    except requests.RequestException as exc:
                        logger.debug("[%s node %d/%d] Poll error: %s",
                                     filename, i, len(nodes), exc)
                        continue
                    if not status_resp.ok:
                        logger.debug("[%s node %d/%d] Poll returned %s: %s",
                                     filename, i, len(nodes),
                                     status_resp.status_code,
                                     status_resp.text[:300])
                        continue

                    last_status_data = status_resp.json()
                    status = last_status_data.get("status", "unknown")
                    progress = last_status_data.get("progress", "")
                    logger.debug(
                        "[%s node %d/%d] Job %s: status=%s %s (attempt %d/%d)",
                        filename, i, len(nodes),
                        job_id, status,
                        f"({progress}%)" if progress else "",
                        attempt + 1, MAX_POLL_ATTEMPTS,
                    )

                    if status == "completed":
                        logger.info("[%s] transcode done in %.0fs", filename,
                                    time.monotonic() - job_started)
                        break
                    if status == "failed":
                        logger.debug(
                            "[%s node %d/%d] Job failed: %s  full_response=%s",
                            filename, i, len(nodes),
                            last_status_data.get("error", "unknown"),
                            last_status_data)
                        break

                if status != "completed":
                    reason = (f"job {job_id} ended with status={status}, "
                              f"last_response={last_status_data}")
                    logger.debug(
                        "[%s node %d/%d] %s — %s — trying next",
                        filename, i, len(nodes), node, reason,
                    )
                    node_errors.append({"node": node, "step": "poll",
                                        "job_id": job_id, "reason": reason})
                    continue

                # --- Step 3: Download the file ---
                download_url = f"{base_url}/{job_id}/download"
                logger.debug("[%s node %d/%d] GET %s",
                             filename, i, len(nodes), download_url)
                try:
                    dl_resp = requests.get(
                        download_url, params=params, stream=True)
                except requests.RequestException as exc:
                    reason = f"connection error on download: {exc}"
                    logger.debug("[%s node %d/%d] %s — %s",
                                 filename, i, len(nodes), node, reason)
                    node_errors.append({"node": node, "step": "download",
                                        "job_id": job_id, "reason": reason})
                    continue

                if not dl_resp.ok:
                    reason = (f"HTTP {dl_resp.status_code} on download GET, "
                              f"body={dl_resp.text[:500]}")
                    logger.debug(
                        "[%s node %d/%d] %s — %s",
                        filename, i, len(nodes), node, reason)
                    node_errors.append({"node": node, "step": "download",
                                        "job_id": job_id, "reason": reason})
                    continue

                size = _stream_to_file(dl_resp, output_path, filename)
                if size == 0:
                    output_path.unlink()
                    reason = "downloaded file is empty (0 bytes)"
                    logger.debug("[%s node %d/%d] %s — %s",
                                 filename, i, len(nodes), node, reason)
                    node_errors.append({"node": node, "step": "download",
                                        "job_id": job_id, "reason": reason})
                    continue

                logger.info("Saved: %s (%.2f MB)",
                            output_path, size / (1024 * 1024))
                return output_path

            if rep_rejected and rep_idx + 1 < len(video_reps):
                logger.info("Rep %s rejected (400) for %s — trying next rep",
                            video_rep, filename)
                continue

            # Any other failure is not the rep's fault; stop here.
            break

        # --- All reps/nodes exhausted: one INFO line, details at DEBUG ---
        logger.info("FAILED %s — all representations and nodes exhausted",
                    filename)
        logger.debug(
            "Failure details for %s (qid=%s, qhash=%s, "
            "start_ms=%s, end_ms=%s, offering=%s, format=%s, last_rep=%s):",
            filename, content_id, content.qhash,
            start_ms, end_ms, offering, format, video_rep,
        )
        for j, err in enumerate(node_errors, 1):
            logger.debug(
                "  [%d/%d] node=%s  step=%s  job_id=%s  reason=%s",
                j, len(node_errors),
                err["node"], err["step"],
                err.get("job_id", "N/A"), err["reason"],
            )
        return None

    def representations(self, content_id: str,
                        offering: str | None = None) -> list[str]:
        """Video rendition ids for `content_id`, lowest bandwidth first."""
        return self._get_content(content_id).default_representations(offering)[0]

    def download_audio_track(
        self,
        content_id: str,
        output_dir: str | Path,
        offering: str | None = None,
        allow_trimmed: bool = False,
    ) -> Path:
        """Download the whole audio track from an audio-only HLS playlist.

        The only true audio-only route: ffmpeg copies the audio
        representation's segments, with no transcode job. The track is taken
        from t=0 without seeking, since ``-c copy`` seeks snap to segment
        boundaries.

        The playlist comes from Content.resolve_audio_playlist, which prefers
        offerings whose playout starts at mezzanine 0. The chosen offering
        and its entry point are recorded in ``audio_full.json``.

        Returns:
            Path to ``audio_full.mp4`` in output_dir.

        Raises:
            TrimmedOfferingError: every ffmpeg-readable offering is trimmed
                and ``allow_trimmed`` is False.
            Any other exception (no usable HLS playout, ffmpeg failure, empty
            output) lets download_parts fall back to media/files chunks.
        """
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        output_path = out_dir / AUDIO_TRACK_FILENAME
        if output_path.exists():
            self._warn_cached_track(output_path)
            return output_path

        content = self._get_content(content_id)
        playout, playlist_url = content.resolve_audio_playlist(
            offering, allow_trimmed=allow_trimmed)
        logger.info("Audio track for %s from offering %s",
                    content_id, playout.describe())

        tmp_path = output_path.with_suffix(".part")
        cmd = [
            ffmpeg_binary(), "-y", "-loglevel", "error",
            "-i", playlist_url,
            "-c", "copy",
            "-f", "mp4", str(tmp_path),
        ]
        logger.info("Downloading audio track for %s via HLS → %s",
                    content_id, output_path)
        try:
            subprocess.run(
                cmd, check=True, capture_output=True, text=True,
                timeout=FFMPEG_TIMEOUT,
            )
            if not tmp_path.exists() or tmp_path.stat().st_size == 0:
                raise RuntimeError(
                    f"ffmpeg produced empty output for {content_id}")
            tmp_path.rename(output_path)
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                f"ffmpeg failed for {content_id}: {exc.stderr[:500]}"
            ) from exc
        finally:
            # Removes partial output on failure; a no-op after the rename.
            tmp_path.unlink(missing_ok=True)

        write_audio_track_provenance(out_dir, {
            "content_id": content_id,
            "version_hash": content.qhash,
            "offering": playout.offering,
            "playout_key": playout.key,
            "entry_point_s": playout.entry_s,
            "exit_point_s": playout.exit_s,
            # "mezzanine": file time == Tagstore time. "offering": the file
            # starts entry_point_s into the mezzanine (segment alignment can
            # move the true start ~2 s earlier; measure before relying on it).
            "timeline": "offering" if playout.trimmed else "mezzanine",
            "downloaded_at": datetime.now(timezone.utc).isoformat(
                timespec="seconds"),
        })

        size = output_path.stat().st_size
        logger.info("Saved: %s (%.2f MB)", output_path, size / MB)
        return output_path

    @staticmethod
    def _warn_cached_track(track_path: Path) -> None:
        warn_cached_track(track_path)

    def download_mezzanine_parts(
        self,
        content_id: str,
        output_dir: str | Path,
        secret: str,
        language: str = "en",
        workers: int | None = None,
    ) -> list[Path]:
        """Download the audio stream's mezzanine parts, in stream order.

        The route content-tagger uses: part i sits at i x part_duration from
        0, the timeline the tags are on, so clips cut from the concatenated
        parts line up with them whatever the offerings' entry points say.

        Parts are decrypted client-side by the ``elv`` CLI, which needs the
        tenancy's signing key (``secret``); a bearer token cannot decrypt
        them. The English 5.1 stream is preferred over stereo.

        Returns the part files (``<index:04d>_<hash>.m4a``) in temporal
        order, also listed in ``parts.json`` so later runs skip the fabric.
        """
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        cached = cached_mezzanine_parts(out_dir)
        if cached:
            logger.info("Mezzanine parts cached: %d parts in %s", len(cached), out_dir)
            return cached

        parts = PartDownloader(
            secret=secret, config_url=self._config_url,
            workers=workers or PARTS_WORKERS,
        )
        library_id = parts.library_id(content_id)
        streams, transcodes = parts.fetch_metadata(content_id)
        audio = [entry for entry in select_streams(build_plan(streams, transcodes), language)
                 if entry["codec_type"] == "audio"]
        if not audio:
            raise RuntimeError(
                f"no {language!r} audio stream with parts for {content_id}")
        stream = audio[0]
        hashes = list(stream["parts"])
        files = [out_dir / f"{index:04d}_{part_hash}.m4a"
                 for index, part_hash in enumerate(hashes)]
        logger.info(
            "Mezzanine parts for %s: stream %s (%s, %s), %d parts x %.3f s -> %s",
            content_id, stream["stream"], stream.get("language") or "?",
            stream.get("channel_layout") or "?", len(files),
            stream["part_duration"], out_dir,
        )

        started = time.monotonic()
        statuses: dict[Path, str] = {}
        # The first read asks the content contract for access, and concurrent
        # first reads race on that transaction. Land the grant with one part,
        # then run the rest in parallel.
        statuses[files[0]] = parts.download_part(content_id, hashes[0], files[0], library_id)
        with ThreadPoolExecutor(max_workers=parts.workers) as pool:
            futures = {
                pool.submit(parts.download_part, content_id, part_hash, path, library_id): path
                for path, part_hash in list(zip(files, hashes))[1:]
            }
            for done, future in enumerate(as_completed(futures), 2):
                statuses[futures[future]] = future.result()
                if done % 25 == 0 or done == len(files):
                    logger.info("  parts %d/%d (%.0fs)", done, len(files),
                                time.monotonic() - started)
        failed = [path.name for path in files if statuses.get(path) == "failed"]
        if failed:
            raise RuntimeError(
                f"{len(failed)}/{len(files)} mezzanine parts failed for "
                f"{content_id}: {', '.join(failed[:3])}")

        (out_dir / PARTS_SIDECAR).write_text(json.dumps({
            "content_id": content_id,
            "library_id": library_id,
            "stream": stream["stream"],
            "language": stream.get("language"),
            "channel_layout": stream.get("channel_layout"),
            "part_duration_s": stream["part_duration"],
            "parts": [path.name for path in files],
            "timeline": "mezzanine",  # part i at i x part_duration_s
            "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }, indent=2) + "\n")
        logger.info("Saved %d mezzanine parts (%.1f MB) in %.0fs",
                    len(files), sum(p.stat().st_size for p in files) / MB,
                    time.monotonic() - started)
        return files

    def download_parts(
        self,
        content_id: str,
        output_dir: str,
        chunk_ms: int = 300_000,
        workers: int = CHUNK_WORKERS,
        allow_trimmed: bool = False,
        secret: str | None = None,
    ) -> list[Path]:
        """Download content audio on the Tagstore timeline.

        Reuses a complete ``parts.json`` part set, or a cached
        ``audio_full.mp4``, in output_dir. Otherwise, in order:

        1. Mezzanine parts (download_mezzanine_parts), when a signing key is
           available -- ``secret`` or $ELV_SECRETS.
        2. Resume a partial media/files chunk cache, if there is one.
        3. The audio-only HLS track (download_audio_track), from an
           untrimmed offering only.
        4. media/files chunks (_download_parts_chunked) when route 3 fails.

        TrimmedOfferingError is re-raised, not a reason to fall back: chunks
        would come from the same trimmed offering. ``allow_trimmed`` accepts
        a trimmed offering on routes 2 to 4.

        Returns:
            Audio files in temporal order: one per part, or a single track.
        """
        out_dir = Path(output_dir)

        cached_parts = cached_mezzanine_parts(out_dir)
        if cached_parts:
            logger.info("Using %d cached mezzanine parts in %s", len(cached_parts), out_dir)
            return cached_parts

        full_path = out_dir / AUDIO_TRACK_FILENAME
        if full_path.exists():
            self._warn_cached_track(full_path)
            return [full_path]

        signing_key = find_secret(secret)
        if signing_key:
            try:
                return self.download_mezzanine_parts(
                    content_id, out_dir, signing_key)
            except Exception:
                logger.warning(
                    "Mezzanine parts download failed for %s — falling back "
                    "to the HLS audio track", content_id, exc_info=True)
        else:
            logger.info(
                "No signing key ($ELV_SECRETS): mezzanine parts cannot be "
                "decrypted, using the HLS audio track for %s", content_id)

        if out_dir.is_dir() and next(out_dir.glob(CHUNK_GLOB), None):
            logger.info("Resuming existing media/files chunk cache in %s",
                        out_dir)
            return self._download_parts_chunked(
                content_id, out_dir, chunk_ms, workers,
                allow_trimmed=allow_trimmed)

        try:
            return [self.download_audio_track(
                content_id, out_dir, allow_trimmed=allow_trimmed)]
        except TrimmedOfferingError:
            raise
        except Exception:
            logger.warning(
                "HLS audio-only download failed for %s — falling back to "
                "media/files chunking", content_id, exc_info=True)
            return self._download_parts_chunked(
                content_id, out_dir, chunk_ms, workers,
                allow_trimmed=allow_trimmed)

    def _download_parts_chunked(
        self,
        content_id: str,
        out_dir: Path,
        chunk_ms: int,
        workers: int = CHUNK_WORKERS,
        allow_trimmed: bool = False,
    ) -> list[Path]:
        """Download content audio in fixed-size chunks via the media/files API.

        Needs only a clear offering and a token: the fabric transcodes, so no
        signing key is involved. Chunks are named ``{index:05d}_{start}-
        {end}.mp4`` (see CHUNK_GLOB), so name order is temporal order, and
        chunks already on disk are skipped.

        Args:
            content_id:    Object ID (iq__...) or version hash (hq__...).
            out_dir:       Directory for the chunk files (created if absent).
            chunk_ms:      Duration of each chunk in ms.
            workers:       Chunks in flight. Each is its own transcode job on
                           the fabric, so they overlap rather than queue.
            allow_trimmed: Accept a trimmed offering (clip times are then on
                           the offering's timeline, not the Tagstore one).

        Returns:
            Chunk files in temporal order.
        """
        out_dir.mkdir(parents=True, exist_ok=True)

        content = self._get_content(content_id)
        # One clear offering for the duration lookup and every chunk. A
        # trimmed one is refused unless allowed: media/files clip times are on
        # the offering's timeline too.
        offering = content.resolve_offering(
            clear_only=True, require_untrimmed=not allow_trimmed)
        total_ms = content.total_duration_ms(offering)
        n_chunks = (total_ms + chunk_ms - 1) // chunk_ms
        logger.info(
            "Content %s: total %d ms → %d chunks of %d ms, %d at a time",
            content_id, total_ms, n_chunks, chunk_ms, workers,
        )

        # media/files rejects an end at or past the content length.
        max_end = total_ms - 100

        def fetch(i: int) -> Optional[Path]:
            chunk_start = i * chunk_ms
            chunk_end = min(chunk_start + chunk_ms, max_end)
            chunk_path = out_dir / f"{i + 1:05d}_{chunk_start}-{chunk_end}.mp4"

            if not chunk_path.exists():
                logger.info(
                    "Downloading audio chunk %d/%d: %d-%d ms",
                    i + 1, n_chunks, chunk_start, chunk_end,
                )
                tmp_path = self.download(
                    content_id=content_id,
                    start_ms=chunk_start,
                    end_ms=chunk_end,
                    output_dir=str(out_dir),
                    offering=offering,
                    audio_only=True,
                )
                if tmp_path:
                    tmp_path.rename(chunk_path)

            return chunk_path if chunk_path.exists() else None

        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            # map keeps results in chunk order, so the list stays temporal.
            paths = [path for path in pool.map(fetch, range(n_chunks)) if path]
        logger.info("%d/%d chunks in %.0fs", len(paths), n_chunks,
                    time.monotonic() - started)
        return paths

    def download_audio(
        self,
        content_id: str,
        start_ms: int,
        end_ms: int,
        output_dir: str = "downloads",
        sample_rate: int = 16_000,
        offering: str | None = None,
        format: str = "mp4",
    ) -> Optional[Path]:
        """Download a time range audio-only and convert it to mono WAV.

        Returns:
            Path to the .wav file, or None when the download fails.
        """
        from .media import extract_audio

        out_dir = Path(output_dir)
        wav_path = out_dir / f"{content_id}_{start_ms}-{end_ms}.wav"
        if wav_path.exists():
            return wav_path

        video_path = self.download(
            content_id=content_id,
            start_ms=start_ms,
            end_ms=end_ms,
            output_dir=output_dir,
            offering=offering,
            format=format,
            audio_only=True,
        )
        if video_path is None:
            return None

        return extract_audio(
            video_path=video_path,
            sample_rate=sample_rate,
        )
