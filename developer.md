# elv_content_py: developer notes

Why the download and metadata code is shaped the way it is: fabric behaviour found the hard way, the measurements behind the defaults, and the decisions that followed. [README.md](README.md) covers usage; read this before changing `downloader.py`, `content.py`, `parts.py` or `extractor.py`.

## The constraint: audio on the Tagstore timeline

The Tagstore tracks (`auto_captions`, `speech_to_text`, `celebrity_detection`) are produced by content-tagger from the title's mezzanine parts, with part *i* placed at *i* × part_duration from 0. Any audio a caller cuts clips from has to be on that same timeline. If it is not, every clip lands on the wrong speech and nothing raises an error; downstream, the only symptom is a speaker bank that makes no sense.

`ContentDownloader.download_parts()` therefore tries its routes in timeline-safety order, not speed order:

| Order | Route | Timeline | Needs |
| --- | --- | --- | --- |
| 0 | Cached `parts.json` part set, or cached `audio_full.mp4` | whatever the sidecar records | nothing |
| 1 | Mezzanine parts (`download_mezzanine_parts`) | mezzanine, by construction | signing key |
| 2 | Resume a partial media/files chunk cache | the offering's | token, clear offering |
| 3 | Audio-only HLS track (`download_audio_track`) | the offering's; untrimmed only | token |
| 4 | media/files chunks (`_download_parts_chunked`) | the offering's | token, clear offering |

`TrimmedOfferingError` from route 3 is re-raised rather than treated as a reason to fall back: route 4 would use the same trimmed offering and reproduce the offset.

## Incident: The Social Network, 88 s off (fixed 2026-09-22)

The Social Network (`iq__31Vb2EuLfZtjyX1ge1rSRoGL87DX`) has an `all_clear` offering with an `entry_point_rat` of 90 s. The HLS audio track downloaded from it started there, so every caption clip was cut 88 s away from its caption. The 2 s difference comes from HLS segment alignment.

What changed as a result:

- `Content.offering_trim()` reads `entry_point_rat` / `exit_point_rat` for each offering, with `resolve_links=True` so links such as `default_clear` → `default` resolve server-side. A value that is present but unreadable counts as trimmed.
- `_rank_playouts()` ranks untrimmed offerings first, then exit-only trims, then offerings with an entry point, whatever their playout format. A correct timeline beats a faster or clearer playout.
- When only trimmed offerings qualify, the download raises `TrimmedOfferingError` unless `allow_trimmed=True`, in which case the caller owns the offset (`Playout.entry_s`).
- Every HLS download writes `audio_full.json` with the offering, playout key, entry/exit points and `timeline: mezzanine | offering`. A cached track without that sidecar predates it and is reused with a warning, because nothing is known about its timeline.
- Caveat: a file on the offering timeline can start up to ~2 s before `entry_point_s` because of segment alignment. Measure the offset before applying it.

The elv-speaker pipeline adds its own guard as well: it checks the decoded audio against the STT word times before cutting clips (`src/timeline_align.py` in the parent repo).

## Mezzanine parts

This is the route content-tagger itself uses, so it is on the tag timeline by construction.

- **Where the parts are listed.** Under `offerings/default/playout/streams` → `transcodes`. Legacy objects without a `transcodes` subtree keep codec, rate and sources inline under `offerings/default/media_struct/streams`.
- **Parts join exactly.** Consecutive parts join sample-exactly: on The Social Network there were 0 samples of gap at a part boundary.
- **Decryption needs the signing key.** Parts are encrypted at rest and decrypted client-side by `elv content part read --decryption-mode decrypt`. A bearer token alone fails with `fixed_authorizer has no kms registry`.
- **An undecrypted part looks fine at a glance.** It has the right length but is not media. `media.looks_like_mp4()` checks for an `ftyp` box on every cached and freshly downloaded part, and anything else is deleted and refetched.
- **The first read races.** It asks the content contract for access on-chain. Concurrent first reads race on that transaction, and all but one fail with `WaitTransact … already known`. The fix has three parts:
  - `download_mezzanine_parts` reads one part on its own before starting the worker pool.
  - `elv_token.TRANSIENT_ACCESS_MARKERS` marks that error as retryable, with backoff.
  - A real refusal (`PERMISSION_MARKERS`) fails at once, because each attempt costs a reverted transaction.
- **English 5.1 is preferred over stereo.** Dialogue lives in the discrete front-centre channel.

## Audio-only HLS track

- **The only true audio-only route.** The audio representation's HLS media playlist holds audio-only fMP4 segments, which ffmpeg copies with `-c copy` and no transcode job. That is about 3 MB/min, against about 63 MB/min from media/files, which transcodes a video representation even when none is requested.
- **Always download from t=0.** A `-c copy` seek snaps to ~2 s segment boundaries, which would skew a timeline that is assumed to be contiguous from 0.
- **Which playouts ffmpeg can read.**
  - `hls-clear` works.
  - `hls-aes128` works too: whole-segment AES with the key served over HTTP, which ffmpeg decrypts natively. This covers DRM-only titles.
  - `hls-sample-aes`, `hls-fairplay` and Widevine need a DRM licence, so they cannot be used.
- **Don't cache the playlist URL.** It embeds a short-lived session token minted by `options.json`, so resolve it again in every session.

## media/files API quirks

- **It is an asynchronous job.** POST creates the job, the downloader polls it every 5 s and gives up after 120 polls (10 min), then GETs `/download`.
- **Unknown fields are rejected.** The request body must contain only fields the API knows. Sending `filename` returned `json: unknown field "filename"`.
- **A 400 on job creation means the representation is invalid.** It fails the same way on every node, so the downloader moves to the next representation instead of the next node.
- **Short clips break the video transcoder.** They fail with `translateToMuxSpec`, so `audio_only=True` omits the video representation entirely.
- **The end time must fall inside the content.** An end at or past the content length is rejected, so the chunker clamps it to `total - 100 ms`.
- **Clear offerings only.** The API cannot transcode DRM content, so every call resolves a clear offering (`dash-clear` / `hls-clear`) once and reuses it.

## `elv` CLI and tokens

- **Why the CLI mints tokens.** `elv content token create --state-channel` resolves the signer's group memberships into the token's `ctx["elv:groupIds"]`, and non-public metadata reads (`meta/transcodes`, `meta/offerings/...`) are authorized against them. A hand-rolled token with an empty `ctx` can still read `meta/public/*` and playout, but is rejected there.
- **Some calls need the key itself.** An `--update` token is an on-chain transaction and costs gas. Resolving a library id is a blockchain read, so it needs the signing key too; a token supplied from outside cannot do it.
- **Parsing CLI failures.** A failure prints a banner, then a `cause:` chain, then a Go stack trace. Truncating from the head, as the code once did (`[:300]`), cut into the banner and lost the root cause. `elv_error()` keeps the chain and drops the trace (2026-09-09, `eefa817`).

## Title metadata (`extractor.py`)

- **Where the cast is read from.** The extractor tries, in order:
  1. `talent.actor` / `voice` / `host` with `{name}`.
  2. `talent.cast` + `talent.cast_mdb` with `{talent_first_name, talent_last_name, character_name}`.
  3. The same pattern on `info.ipm_talent`. Some titles keep only crew under `talent`.
  4. The `ml` subtree's `v0.content_params.castlist`.
- **Merging and de-duplication.** `cast` is the curated list and keeps its credit order. `cast_mdb` is often longer and adds only actors missing from `cast`. Some titles repeat every `cast` entry with the full name packed into `talent_first_name`, so entries are de-duplicated case- and whitespace-insensitively.
- **Fallbacks worth knowing.**
  - Sports and VOD objects keep their title at `public/name`, not `public/asset_metadata/display_title`.
  - A failed read of the `ml` subtree is logged at DEBUG only: an auth or network failure there looks the same as a missing subtree.
- **The cast string format is an interface.** Entries are saved as strings: `Name plays Role`, `Name voices Role`, `Name (Host)`, or a bare name. elv-speaker parses these back (`title_cast_entries`, `extract_title_names`, and its own-STT biasing), so changing the format breaks those consumers without any error.

## Dependency boundaries

`config.py`, `elv_token.py`, `parts.py` and `media.py` import nothing beyond `requests` and the standard library, so token minting and part downloads work without `elv_client_py` (not on PyPI) or `loguru`. For the same reason `__init__.py` imports `Content`, `ContentDownloader`, `TitleExtractor` and `parse_title_metadata` lazily, on first use. Keep any new import in those four modules to the same rule.

`media.ffmpeg_binary()` looks for ffmpeg on PATH, else in the `imageio-ffmpeg` wheel, and only when it is called, so the package imports without either. `elv_token.py` and `parts.py` also run as plain scripts (`python parts.py <qid> ...`), which is why they fall back from relative to absolute imports.

## Files on disk

| File | Written by | Contents |
| --- | --- | --- |
| `audio_full.mp4` | `download_audio_track` | whole HLS audio track |
| `audio_full.json` | `download_audio_track` | content id, version hash, offering, playout key, entry/exit s, `timeline`, download time |
| `NNNN_<hash>.m4a` | `download_mezzanine_parts` | one decrypted mezzanine audio part |
| `parts.json` | `download_mezzanine_parts` | content id, library id, stream, language, channel layout, `part_duration_s`, ordered part names, `timeline: mezzanine`, download time |
| `NNNNN_<start>-<end>.mp4` | `_download_parts_chunked` | one media/files chunk; `CHUNK_GLOB` must match this name |

A `parts.json` set counts as cached only when every listed part exists, is non-empty and passes the `ftyp` check (`cached_mezzanine_parts`).

## Measurements behind the defaults

| What | Result | Used for |
| --- | --- | --- |
| Mezzanine parts, 235-part title, 8 workers | ~2 s per part | `PARTS_WORKERS = 8` |
| One part read (`parts.py`) | ~7 s for ~3.7 MB, mostly waiting on the KMS and node | overlapping reads pays off |
| Parallel part reads, 12 parts | 1 / 4 / 8 workers: 91 / 34 / 20 s on an object that parallelizes; 89 / 88 / 20–76 s on one that mostly does not (README) | `WORKERS = 8` |
| `elv` process resident memory | ~110 MB each | lower `--workers` on small machines |
| HLS audio vs media/files output | ~3 MB/min vs ~63 MB/min | HLS preferred over chunks |
| Part boundary gap (The Social Network) | 0 samples | parts can be concatenated directly |

## History

| Date | Commit | Change |
| --- | --- | --- |
| 2026-03-24 to 03-31 | `c3b509f` … `d6bae10` | media/files downloader and metadata fetch; clips shorter than 1 s |
| 2026-04-29 to 08-12 | `32f141d` … `ba9951a` | metadata from several schemas, per-title cache, field-name fallbacks |
| 2026-08-14 | `74b4c0e` | `elv` CLI tokens and whole-object part download (`parts.py`), for content with no clear offering |
| 2026-08-21 | `b6f8c86` | part-download progress and output root; clip representation choice; concurrent media/files chunks |
| 2026-09-09 | `eefa817` | decryption mode selection; `elv_error()`; permission errors not retried |
| staged, uncommitted | | whole-title HLS audio track, media/files chunk fallback, `ipm_talent` cast |
| working tree, uncommitted | | mezzanine parts route, trimmed-offering ranking and `TrimmedOfferingError`, provenance sidecars, `hls-aes128`, access-race retry |
