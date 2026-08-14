"""Local ffmpeg helpers and media sniffing.

ffmpeg is resolved from PATH, falling back to the imageio_ffmpeg wheel, and only
when it is needed, so the rest of the package imports fine without either.
"""

import logging
import shutil
import struct
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

TIMEOUT = 300


def ffmpeg_binary() -> str:
    found = shutil.which("ffmpeg")
    if found:
        return found
    try:
        import imageio_ffmpeg

        return imageio_ffmpeg.get_ffmpeg_exe()
    except ImportError:
        raise RuntimeError(
            "ffmpeg not found; install ffmpeg or pip install imageio-ffmpeg"
        )


def looks_like_mp4(path: Path) -> bool:
    """True if the file starts with an `ftyp` box, as a decrypted media part
    does. An undecrypted part is the right length but random bytes."""
    try:
        with Path(path).open("rb") as handle:
            header = handle.read(8)
        if len(header) < 8:
            return False
        size = struct.unpack(">I", header[:4])[0]
        return header[4:8] == b"ftyp" and 8 <= size <= 1024
    except OSError:
        return False


def is_wav(path: Path) -> bool:
    try:
        with Path(path).open("rb") as handle:
            header = handle.read(12)
        return len(header) == 12 and header[:4] == b"RIFF" and header[8:12] == b"WAVE"
    except OSError:
        return False


def _run_ffmpeg(arguments: list[str], destination: Path, ffmpeg: str | None = None) -> bool:
    command = [ffmpeg or ffmpeg_binary(), "-hide_banner", "-loglevel", "error", "-y"]
    command += arguments
    command.append(str(destination))
    result = subprocess.run(command, capture_output=True, text=True, timeout=TIMEOUT)
    if result.returncode == 0 and is_wav(destination):
        return True
    if destination.exists():
        destination.unlink()
    logger.error("ffmpeg failed for %s: %s", destination.name, result.stderr.strip()[:300])
    return False


def extract_center(source, destination, sample_rate: int | None = None,
                   ffmpeg: str | None = None) -> str:
    """Downmix the front-center channel of a surround part to mono PCM.

    Returns "cached", "extracted" or "failed".
    """
    source, destination = Path(source), Path(destination)
    if destination.exists() and destination.stat().st_size > 0 and is_wav(destination):
        return "cached"

    arguments = ["-i", str(source), "-filter_complex", "pan=mono|c0=FC", "-c:a", "pcm_s16le"]
    if sample_rate:
        arguments += ["-ar", str(sample_rate)]
    return "extracted" if _run_ffmpeg(arguments, destination, ffmpeg) else "failed"


def extract_audio(video_path, sample_rate: int = 16_000, destination=None,
                  ffmpeg: str | None = None) -> Path | None:
    """Downmix a media file to a mono PCM wav. Returns the path, or None."""
    video_path = Path(video_path)
    destination = Path(destination) if destination else video_path.with_suffix(".wav")
    if destination.exists() and destination.stat().st_size > 0 and is_wav(destination):
        return destination

    arguments = ["-i", str(video_path), "-vn", "-ac", "1", "-c:a", "pcm_s16le",
                 "-ar", str(sample_rate)]
    return destination if _run_ffmpeg(arguments, destination, ffmpeg) else None
