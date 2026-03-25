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
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

POLL_INTERVAL = 5       # seconds between status checks
MAX_POLL_ATTEMPTS = 120  # max wait


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
    ) -> Optional[Path]:
        """Download a video segment from the content fabric.

        Args:
            content_id: Object ID (iq__...) or version hash (hq__...).
            start_ms: Clip start time in milliseconds.
            end_ms: Clip end time in milliseconds.
            output_dir: Directory to save the downloaded file.
            offering: Playout offering (default: "default_clear").
            format: Container format (default: "mp4").

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

        # Auto-select representation and audio from playout options
        video_rep, audio_rep = content.default_representations(offering)
        logger.info("Auto-selected representation=%s  audio=%s",
                    video_rep, audio_rep)

        start_s = f"{start_ms / 1000:.4f}s"
        end_s = f"{end_ms / 1000:.4f}s"

        body = {
            "format": format,
            "offering": offering,
            "filename": filename,
            "start_ms": start_s,
            "end_ms": end_s,
            "representation": video_rep,
            "audio": audio_rep,
        }

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        params = {"authorization": self._token}

        nodes = content._client.fabric_uris
        logger.info("Downloading: %s", filename)

        for node in nodes:
            base_url = f"{node}/q/{content.qhash}/call/media/files"

            # --- Step 1: Create transcode job ---
            logger.info("POST %s  body=%s", base_url, body)
            resp = requests.post(
                base_url, json=body, headers=headers, params=params
            )
            if not resp.ok:
                logger.warning(
                    "Node %s returned %s on job create: %s — trying next",
                    node, resp.status_code, resp.text[:500],
                )
                continue

            job_data = resp.json()
            job_id = job_data.get("job_id")
            if not job_id:
                logger.error(
                    "No job_id in response from %s: %s", node, job_data)
                continue

            logger.info("Job created: %s on %s", job_id, node)

            # --- Step 2: Poll until completed ---
            status_url = f"{base_url}/{job_id}"
            status = "processing"
            for attempt in range(MAX_POLL_ATTEMPTS):
                time.sleep(POLL_INTERVAL)
                status_resp = requests.get(status_url, params=params)
                if not status_resp.ok:
                    logger.warning("Poll returned %s", status_resp.status_code)
                    continue

                status_data = status_resp.json()
                status = status_data.get("status", "unknown")
                progress = status_data.get("progress", "")
                logger.info(
                    "Job %s: status=%s %s (attempt %d/%d)",
                    job_id, status,
                    f"({progress}%)" if progress else "",
                    attempt + 1, MAX_POLL_ATTEMPTS,
                )

                if status == "completed":
                    break
                if status == "failed":
                    logger.error(
                        "Job failed: %s", status_data.get("error", "unknown"))
                    break

            if status != "completed":
                logger.error(
                    "Job %s did not complete on node %s (status=%s) — trying next",
                    job_id, node, status,
                )
                continue

            # --- Step 3: Download the file ---
            download_url = f"{base_url}/{job_id}/download"
            logger.debug("GET %s", download_url)
            dl_resp = requests.get(
                download_url, params=params, stream=True)
            if not dl_resp.ok:
                logger.error(
                    "Download GET returned %s on node %s", dl_resp.status_code, node)
                continue

            with open(output_path, "wb") as f:
                for chunk in dl_resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            size = output_path.stat().st_size
            if size == 0:
                output_path.unlink()
                logger.error("Downloaded file is empty for %s", filename)
                continue

            logger.info(
                "Saved: %s (%.2f MB)", output_path, size / (1024 * 1024))
            return output_path

        logger.error("All nodes failed for %s", filename)
        return None

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
        from src.utils import extract_audio

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
        )
        if video_path is None:
            return None

        return extract_audio(
            video_path=video_path,
            sample_rate=sample_rate,
        )
