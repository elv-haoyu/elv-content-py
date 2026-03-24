"""Download video segments from Eluvio Content Fabric via the media/files API."""

import requests
from pathlib import Path
from typing import Optional

from .content import Content, DEFAULT_CONFIG_URL


class ContentDownloader:
    """Download video/audio clips from Eluvio content fabric."""

    def __init__(self, auth_token: str, config_url: str = DEFAULT_CONFIG_URL):
        self._token = auth_token
        self._config_url = config_url

    def _get_content(self, qhit: str) -> Content:
        return Content(qhit, self._token, self._config_url)

    def download(
        self,
        content_id: str,
        start_ms: int,
        end_ms: int,
        output_dir: str = "downloads",
        offering: str = "default_clear",
        format: str = "mp4",
        representation: Optional[str] = None,
        audio: Optional[str] = None,
    ) -> Optional[Path]:
        """Download a video segment from the content fabric.

        Args:
            content_id: Object ID (iq__...) or version hash (hq__...).
            start_ms: Clip start time in milliseconds.
            end_ms: Clip end time in milliseconds.
            output_dir: Directory to save the downloaded file.
            offering: Playout offering (default: "default_clear").
            format: Container format (default: "mp4").
            representation: Video representation string (optional).
            audio: Audio track identifier (optional).

        Returns:
            Path to the downloaded file, or None on failure.
        """
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        content = self._get_content(content_id)

        filename = f"{content.qid}_{start_ms}-{end_ms}.{format}"
        output_path = out_dir / filename

        if output_path.exists():
            print(f"Already exists: {output_path}")
            return output_path

        # Convert ms to seconds for the API
        start_s = f"{start_ms / 1000:.4f}s"
        end_s = f"{end_ms / 1000:.4f}s"

        url = f"{content.fabric_node}/q/{content.qhash}/call/media/files"

        body = {
            "format": format,
            "offering": offering,
            "filename": filename,
            "start_ms": start_s,
            "end_ms": end_s,
        }
        if representation:
            body["representation"] = representation
        if audio:
            body["audio"] = audio

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        params = {"authorization": self._token}

        print(f"Downloading: {filename}")
        resp = requests.post(
            url, json=body, headers=headers, params=params, stream=True
        )
        resp.raise_for_status()

        with open(output_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"Saved: {output_path} ({size_mb:.2f} MB)")
        return output_path
