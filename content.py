from typing import Any, Dict
import requests

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

    def total_duration_ms(self, offering: str = "default_clear") -> int:
        """Return the total content duration in milliseconds from the DASH manifest."""
        import re

        opts = self.playout_options(offering)
        dash_uri = opts.get("dash-clear", {}).get("uri", "")
        if not dash_uri:
            raise ValueError("No dash-clear playout URI found")

        manifest_url = (
            f"{self.fabric_node}/q/{self.qhash}/rep/playout/{offering}/{dash_uri}"
        )
        resp = requests.get(manifest_url)
        resp.raise_for_status()

        match = re.search(
            r'mediaPresentationDuration="PT(?:(\d+)H)?(?:(\d+)M)?(?:([\d.]+)S)?"',
            resp.text,
        )
        if not match:
            raise ValueError("Could not find mediaPresentationDuration in DASH manifest")

        hours = float(match.group(1) or 0)
        minutes = float(match.group(2) or 0)
        seconds = float(match.group(3) or 0)
        return int((hours * 3600 + minutes * 60 + seconds) * 1000)

    def playout_options(self, offering: str = "default_clear") -> Dict[str, Any]:
        """Fetch playout options for the given offering.

        Returns the raw options dict from:
            GET /q/{qhash}/rep/playout/{offering}/options.json
        """
        url = f"{self.fabric_node}/q/{self.qhash}/rep/playout/{offering}/options.json"
        resp = requests.get(url, params={"authorization": self._token})
        resp.raise_for_status()
        return resp.json()

    def default_representations(
        self, offering: str = "default_clear",
    ) -> tuple[list[str], str]:
        """Return (video_rep_ids, audio_rep_id) for a download.

        Parses the DASH manifest to get the actual representation IDs
        (e.g. 'videovideo_640x360_h264@1055556', 'english_5_1audio_aac@384000').
        Returns video reps sorted by bandwidth (ascending) so callers can
        fall back to the next rep if the lowest one is rejected by the API.
        Prefers english_5_1 > english_stereo for audio.
        """
        import re

        opts = self.playout_options(offering)
        # Find the dash-clear variant to get the manifest URI
        dash_info = opts.get("dash-clear", {})
        dash_uri = dash_info.get("uri", "")
        if not dash_uri:
            raise ValueError("No dash-clear playout URI found")

        # Fetch the DASH manifest
        manifest_url = (
            f"{self.fabric_node}/q/{self.qhash}/rep/playout/{offering}/{dash_uri}"
        )
        resp = requests.get(manifest_url)
        resp.raise_for_status()
        mpd = resp.text

        # Extract all Representation ids with their contentType from parent AdaptationSet
        video_reps: list[tuple[str, int]] = []  # (id, bandwidth)
        audio_reps: list[tuple[str, int]] = []  # (id, bandwidth)

        # Split by AdaptationSet
        for adapt_match in re.finditer(
            r'<AdaptationSet[^>]*contentType="(\w+)"[^>]*>(.*?)</AdaptationSet>',
            mpd, re.DOTALL,
        ):
            content_type = adapt_match.group(1)
            block = adapt_match.group(2)
            for rep_match in re.finditer(
                r'<Representation[^>]*\bbandwidth="(\d+)"[^>]*\bid="([^"]+)"', block,
            ):
                bw = int(rep_match.group(1))
                rep_id = rep_match.group(2)
                if content_type == "video":
                    video_reps.append((rep_id, bw))
                elif content_type == "audio":
                    audio_reps.append((rep_id, bw))

        if not video_reps:
            raise ValueError("No video representations found in DASH manifest")
        if not audio_reps:
            raise ValueError("No audio representations found in DASH manifest")

        # Video: sorted by bandwidth ascending (lowest first)
        video_reps_sorted = [r[0]
                             for r in sorted(video_reps, key=lambda x: x[1])]

        # Audio: prefer english_5_1, fall back to english_stereo
        english_5_1 = [r for r in audio_reps if "english_5_1" in r[0]]
        english_stereo = [r for r in audio_reps if "english_stereo" in r[0]]

        if english_5_1:
            audio_rep = english_5_1[0][0]
        elif english_stereo:
            audio_rep = english_stereo[0][0]
        else:
            available = [r[0] for r in audio_reps]
            raise ValueError(
                f"No English audio track found. Available: {available}"
            )

        return video_reps_sorted, audio_rep

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
