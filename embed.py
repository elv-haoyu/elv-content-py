"""Embed player URLs for the content fabric.

A Python port of ``ElvClient.EmbedUrl`` (elv-client-js,
``src/client/ContentAccess.js``). Pure string building — no fabric calls, no optional dependencies — so this
imports anywhere in the package. 

The two things the JS version does that need the network are left to the caller:

* **the token.** ``EmbedUrl`` mints one with ``CreateSignedToken`` when the
  caller has permission. Pass a state-channel token as *token*; see
  ``elv_token.create_token``.
* **the network name.** ``EmbedUrl`` reads it from ``NetworkInfo()``. It is
  ``main`` for the production fabric, which is the default here.

    >>> embed_url("iq__4Md7rZAcnT7s8V5kQfvmn1ZXiqob",
    ...           clip_start=644.5, clip_end=647.5,
    ...           offerings=["default_clear"], token="ascsj_...")
    'https://embed.v3.contentfabric.io/?p=&net=main&oid=iq__4Md7...'
"""

import base64
import json
from urllib.parse import urlencode

EMBED_BASE_URL = "https://embed.v3.contentfabric.io"

#: ``mt`` — media type.
MEDIA_TYPES = {
    "video": "v",
    "live_video": "lv",
    "audio": "a",
    "image": "i",
    "html": "h",
    "ebook": "b",
    "gallery": "g",
    "link": "l",
}

#: ``ct`` — player controls. ``hide`` is special: it omits the parameter
#: entirely rather than sending a value.
CONTROLS = {
    "autoHide": "h",
    "browserDefault": "d",
    "show": "s",
    "hideWithVolume": "hv",
}


def _b64(text: str) -> str:
    """The client's ``Utils.B64``: standard base64 of the utf-8 bytes."""
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def embed_url(
    object_id: str | None = None,
    *,
    version_hash: str | None = None,
    token: str | None = None,
    network: str = "main",
    media_type: str = "video",
    controls: str = "autoHide",
    clip_start: float | None = None,
    clip_end: float | None = None,
    offerings: list[str] | None = None,
    protocols: list[str] | None = None,
    title: str | None = None,
    description: str | None = None,
    poster_url: str | None = None,
    link_path: str | None = None,
    view_record_key: str | None = None,
    account_watermark: bool = False,
    autoplay: bool = False,
    cap_level_to_player_size: bool = False,
    direct_link: bool = False,
    loop: bool = False,
    muted: bool = False,
    show_share: bool = False,
    show_title: bool = False,
    verify_content: bool = False,
    base_url: str = EMBED_BASE_URL,
    additional_parameters: dict | None = None,
) -> str:
    """Build an embed player URL.

    Args:
        object_id: ``iq__…`` object id. Emitted as ``oid``.
        version_hash: ``hq__…`` version hash. Emitted as ``vid`` and takes
            precedence over *object_id*, matching the JS argument order.
        token: state-channel authorization token, emitted as ``ath``. Without
            one the player loads but cannot read the content.
        network: fabric network name (``main``, ``demo``, …).
        media_type: key of :data:`MEDIA_TYPES`; unknown values fall back to
            video, as the client does.
        controls: key of :data:`CONTROLS`, or ``hide`` to omit the parameter.
        clip_start: clip in point, **in seconds**.
        clip_end: clip out point, in seconds.
        offerings: offerings to play, joined with commas. Worth setting: with
            no ``off`` the player takes the object's default offering, which is
            often the DRM one, so a reviewer who only needs to look at a frame
            is sent down the licence path. Naming a ``_clear`` offering avoids
            it.
        protocols: playout protocols (``hls``, ``dash``), joined with commas.
        title: page title, carried in the base64 ``data`` blob as ``og:title``.
        description: likewise, as ``og:description``.
        link_path: base64 encoded and emitted as ``ln``.
        additional_parameters: appended verbatim, last, so a caller can reach a
            parameter this function does not model.

    Raises:
        ValueError: if neither *object_id* nor *version_hash* is given.
    """
    if not object_id and not version_hash:
        raise ValueError("embed_url needs an object_id or a version_hash")

    params: list[tuple[str, str]] = [("p", ""), ("net", network)]

    if version_hash:
        params.append(("vid", version_hash))
    else:
        params.append(("oid", object_id))

    params.append(("mt", MEDIA_TYPES.get(media_type.lower(), "v")))

    # "hide" is the one controls value with no parameter, so a caller cannot
    # express it by passing a value.
    if controls != "hide" and controls in CONTROLS:
        params.append(("ct", CONTROLS[controls]))

    if clip_start is not None:
        params.append(("start", _seconds(clip_start)))
    if clip_end is not None:
        params.append(("end", _seconds(clip_end)))
    if offerings:
        params.append(("off", ",".join(offerings)))
    if protocols:
        params.append(("ptc", ",".join(protocols)))
    if poster_url:
        params.append(("pst", poster_url))
    if link_path:
        params.append(("ln", _b64(link_path)))
    if view_record_key:
        params.append(("vrk", view_record_key))

    # Valueless flags: present in the URL when on, absent when off.
    for enabled, key in (
        (account_watermark, "awm"),
        (autoplay, "ap"),
        (cap_level_to_player_size, "cap"),
        (direct_link, "dr"),
        (loop, "lp"),
        (muted, "m"),
        (show_share, "sh"),
        (show_title, "st"),
        (verify_content, "vc"),
    ):
        if enabled:
            params.append((key, ""))

    meta_tags = {}
    if title:
        meta_tags["og:title"] = title
    if description:
        meta_tags["og:description"] = description
    if meta_tags:
        params.append(("data", _b64(json.dumps({"meta_tags": meta_tags}))))

    for key, value in (additional_parameters or {}).items():
        params.append((key, str(value)))

    # The token goes last, as it does in the JS, so the readable parameters
    # stay at the front of a URL that is mostly token.
    if token:
        params.append(("ath", token))

    return f"{base_url}/?" + urlencode(params)


def _seconds(value: float) -> str:
    """Clip points as seconds, clamped at zero, without trailing zeros.

    A negative in point is what a caller gets by subtracting lead-in from a
    timestamp near the head of the content; the player treats it as invalid
    rather than as zero.

    Formatted the long way rather than with ``:g``, which carries only six
    significant digits: a clip at 12345.67s renders as "12345.7", silently
    moving the in point by a third of a second, and past 999999s it switches to
    scientific notation, which is not a number the player will read at all.
    Three decimals is a millisecond, which is finer than any frame.
    """
    text = f"{max(0.0, float(value)):.3f}".rstrip("0").rstrip(".")
    return text or "0"
