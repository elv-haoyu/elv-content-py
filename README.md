# elv-content-py

Python helpers for the [Eluvio content fabric](https://eluv.io): auth tokens,
whole-object part downloads, transcoded clip downloads and title metadata.


## Install

Clone (or symlink) it under an importable name -- `elv_content_py`, not
`elv-content-py` -- if you want `import elv_content_py` and `python -m`.

```bash
python3 -m venv elv-content-tool && source elv-content-tool/bin/activate
git clone git@github.com:elv-haoyu/elv-content-py.git elv_content_py
pip install -r elv_content_py/requirements.txt
```


For the token and part paths only, `pip install requests imageio-ffmpeg` is
enough -- neither `elv_client_py` nor `loguru` is imported until `Content`,
`ContentDownloader` or `TitleExtractor` is used.


## Install `elv` CLI

`token` and `parts` shell out to `elv`, the Eluvio Content Fabric CLI. Install it with the script from
[docs.eluv.io/providers/clients](https://docs.eluv.io/providers/clients/):

```bash
curl -s https://docs.eluv.io/installer/elv.sh | bash -s                    # -> ~/bin
curl -s https://docs.eluv.io/installer/elv.sh | bash -s -- -p /opt/eluvio  # elsewhere
```

### Verify

```bash
$ elv --version
Eluvio Content Fabric CLI version release-4.2@a9c5adab... 2025-06-25T00:57:57Z

$ python -c 'from elv_content_py import elv_binary; print(elv_binary())'
/home/<you>/bin/elv
```

The second runs the package's own lookup; when it fails it raises
`RuntimeError: elv CLI not found; looked at $ELV_BIN, on PATH and in ~/bin`.

With a signing key, one part end to end proves CLI, key and fabric access
together -- it should print `parts: downloaded=2 cached=0 failed=0`:

```bash
python -m elv_content_py parts <qid> --secret 0x<hex> --max-parts 1 -o /tmp/elv-check
```

## Auth

Three ways in, tried in that order:

| | |
| --- | --- |
| `--secret 0x<hex>` | the signing key |
| `$ELV_SECRETS` | the same key, from the environment |
| `./token.txt` | an already-minted token, taken from the file's **last line**; `--token` accepts a token or another path, `$ELV_TOKEN_FILE` changes the default |

The key mints its own tokens and is the only thing that can decrypt parts. A
token carries just the access it was minted with -- it cannot mint further
tokens, resolve a library id, or decrypt.

The fabric config URL comes from the `config_url` argument, else
`$ELV_CONFIG_URL`, else the main network. Point it at a single node
(`https://host-<ip>.contentfabric.io/config?self&qspace=main`) to pin every
request to that node.

## Use

```bash
export ELV_SECRETS=0x<hex>                     # or pass --secret to each command

python -m elv_content_py token iq__4Dzv...                    # state-channel, the default
python -m elv_content_py token iq__4Dzv... --reenc            # or --update
python -m elv_content_py parts iq__4Dzv... -o /ml/data/content
python -m elv_content_py parts iq__4Dzv... --token token.txt  # no key, metadata only
python -m elv_content_py title --qids iq__4Dzv...             # token.txt
python -m elv_content_py download --qid iq__4Dzv... --start 0 --end 120000
```

A token is `--state-channel` (read: metadata, playout, parts) unless you ask for
`--reenc` (encrypted content) or `--update` (write; an on-chain transaction, so
it costs gas).

`elv_token.py` and `parts.py` also run as plain scripts from inside the
directory (`python parts.py <qid> --secret 0x<hex>`), with no package import.

```python
from elv_content_py import PartDownloader, create_token, find_secret, load_token

secret = "0x<hex>"                                    # or find_secret() -> $ELV_SECRETS
manifest = PartDownloader(secret=secret).download("iq__4Dzv...", output_root="/ml/data/content")

token = create_token(secret, "iq__...")               # state-channel read token
token = load_token()                                  # or the last line of token.txt
```

`PartDownloader` takes `token=` instead of `secret=` when you only have a token,
but parts of encrypted content cannot be decrypted without a signing key.

## Two ways to get media

`parts` copies the bytes the fabric already stores. `download` asks the fabric to
transcode a time range and hands back one file. They are not interchangeable:

| | `parts` / `PartDownloader` | `download` / `ContentDownloader` |
| --- | --- | --- |
| what you get | the stored parts of a stream, byte for byte | a freshly transcoded clip |
| unit | whole stream, part-aligned (parts run ~30 s) | any `start_ms`--`end_ms` range |
| quality | as mastered; no re-encode | the representation the API picks -- lowest-bandwidth video first |
| how | `elv content part read`, decrypted client-side | `POST /call/media/files` job, poll, then GET the result |
| auth | a signing key: the KMS only releases decryption keys to one | any playout token |
| deps | the `elv` CLI (+ ffmpeg for center extraction) | `elv_client_py` |
| offering | `default` (`playout/streams`, or legacy `media_struct`) | `default_clear` |
| speed | ~7--11 s per part, serial (see below) | one transcode job per clip; polls every 5 s, gives up after 10 min |
| output | one file per part, per stream, plus `manifest.json` | a single `.mp4`/`.wav` per call |
| resumes | yes -- existing parts are skipped and verified | yes -- an existing output file is returned as is |

Use `parts` when you want **a whole title at source quality**: building an ML
dataset, running ASR or shot detection over full features, or anything where
re-encoding loss and per-clip transcode latency would compound. It is also the
only path that gives you the discrete 5.1 center channel.

Use `download` when you want **a few specific moments**: pulling the clip behind
a tag for review, cutting training examples around timestamps, or spot-checking
playout. It is also the fallback when you have no signing key, or when the KMS
will not release decryption keys for an object -- the transcode happens on the
fabric side, so a plain auth token is enough.

`ContentDownloader.download_parts()` sits between the two: it walks a whole title
through the media/files API in fixed-length audio chunks (5 min by default), which
is how you get full-length audio for content whose parts you cannot decrypt.
`download_audio()` wraps a clip download and extracts mono WAV.

### Part downloads in detail

Media is stored as an ordered list of encrypted parts per stream, and decryption
happens client-side, so parts are read through the `elv` CLI -- a plain HTTP GET
returns bytes of the right length that are not media.

`PartDownloader.download()` takes one video stream and one English audio stream
by default, preferring 5.1 over stereo, and extracts the 5.1 front-center channel
(which carries dialogue) to mono WAV. It writes:

```
<output_root>/<qid>/
    manifest.json                            streams, part hashes, durations
    video/0000_hqpe....mp4
    english_5_1__.../0000_hqpe....m4a        as downloaded (5.1)
    english_5_1__..._center/0000_hqpe....wav mono center channel
```

