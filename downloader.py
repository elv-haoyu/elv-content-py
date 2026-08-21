"""Download video segments from Eluvio Content Fabric via the media/files API.

The media/files endpoint is asynchronous:
  1. POST  /call/media/files          -> creates a transcode job, returns {job_id}
  2. GET   /call/media/files/{job_id} -> poll until status == "completed"
  3. GET   /call/media/files/{job_id}/download -> stream the actual file
"""

from .content import Content, DEFAULT_CONFIG_URL
import logging
import time

import requests
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

POLL_INTERVAL = 5       # seconds between status checks
MAX_POLL_ATTEMPTS = 120  # max wait
CHUNK_SIZE = 1 << 20
CHUNK_WORKERS = 4       # concurrent transcode jobs in download_parts
PROGRESS_INTERVAL = 5   # seconds between download progress lines
MB = 1024 * 1024


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


class ContentDownloader:
    """Download video/audio clips from Eluvio content fabric."""

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
        offering: str = "default_clear",
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
            offering: Playout offering (default: "default_clear").
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

        # Auto-select representations from playout options
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

        # When audio_only, skip video rep entirely to avoid video transcoder
        # errors (e.g. short clips that fail translateToMuxSpec).
        if audio_only:
            video_reps = [None]

        # Try each video rep (lowest bandwidth first), fall back on 400
        for rep_idx, video_rep in enumerate(video_reps):
            # No `filename`: the API rejects unknown fields outright
            # (`json: unknown field "filename"`). It only named the local file.
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
                    # 400 means the rep itself is invalid — no point trying
                    # other nodes with the same rep, skip to next rep.
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

            # If the rep was rejected (400), try the next rep
            if rep_rejected and rep_idx + 1 < len(video_reps):
                logger.info("Rep %s rejected (400) for %s — trying next rep",
                            video_rep, filename)
                continue

            # Non-400 failures or last rep: give up
            break

        # --- All reps/nodes exhausted — one-line INFO + detailed DEBUG ---
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
                        offering: str = "default_clear") -> list[str]:
        """Video rendition ids for `content_id`, lowest bandwidth first."""
        return self._get_content(content_id).default_representations(offering)[0]

    def download_parts(
        self,
        content_id: str,
        output_dir: str,
        chunk_ms: int = 300_000,
        workers: int = CHUNK_WORKERS,
    ) -> list[Path]:
        """Download content audio in fixed-size chunks via the media/files API.

        Uses time-based chunking so it works for encrypted content (where raw
        part download is unavailable).  Chunks are named ``{index:05d}_{start}-
        {end}.mp4`` so alphabetical order equals temporal order.

        Already-downloaded chunks are skipped (safe to call repeatedly).

        Args:
            content_id: Object ID (iq__...) or version hash (hq__...).
            output_dir: Directory to save chunk files into (created if absent).
            chunk_ms:   Duration of each audio chunk in ms (default: 5 minutes).
            workers:    Chunks in flight at once. Each is a separate transcode
                        job on the fabric, so they overlap rather than queue.

        Returns:
            List of paths to chunk files in temporal order.
        """
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        content = self._get_content(content_id)
        total_ms = content.total_duration_ms()
        n_chunks = (total_ms + chunk_ms - 1) // chunk_ms
        logger.info(
            "Content %s: total %d ms → %d chunks of %d ms, %d at a time",
            content_id, total_ms, n_chunks, chunk_ms, workers,
        )

        def fetch(i: int) -> Optional[Path]:
            chunk_start = i * chunk_ms
            chunk_end = min(chunk_start + chunk_ms, total_ms)
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
        offering: str = "default_clear",
        format: str = "mp4",
    ) -> Optional[Path]:
        """Download a video segment and extract mono WAV audio.
        Returns:
            Path to the extracted .wav file, or None on download failure.
        """
        from .media import extract_audio

        # Check if wav already exists before downloading
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
