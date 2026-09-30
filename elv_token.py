#!/usr/bin/env python3
"""Create Eluvio fabric authorization tokens via the `elv` CLI.

Tokens are created with:

    elv content token create <qid> --state-channel --secret <key>

The CLI resolves the signer's group memberships and embeds them in the token as
`ctx["elv:groupIds"]`. That is what non-public metadata reads (`meta/transcodes`,
`meta/offerings/...`) are authorized against, so a hand-rolled token with an empty
`ctx` is rejected there even though it can read `meta/public/*` and playout.

Modes, passed as the flag itself:
    --state-channel  off-chain read token (default) -- enough for metadata,
                     playout and part downloads
    --reenc          read with re-encryption, for encrypted content
    --update         write token; requires an on-chain transaction, so it costs
                     gas and should not be issued in bulk

Auth, in order:

    --secret 0x<hex>    the signing key
    $ELV_SECRETS        the same key, from the environment
    ./token.txt         an already-minted token, its last line ($ELV_TOKEN_FILE,
                        or --token, override the path)

Minting needs the key; a token only carries the access it was minted with and
cannot sign new authorizations or decrypt.

Usage:
    export ELV_SECRETS=0x... 
    python elv_token.py iq__4Dzv28AJhNGsK2FkUEf7FyxYcRwH
    or 
    python elv_token.py iq__4Dzv28AJhNGsK2FkUEf7FyxYcRwH --secret 0x...
    python elv_token.py iq__4Dzv28AJhNGsK2FkUEf7FyxYcRwH --reenc
"""

import json
import os
import re
import shutil
import subprocess
import time
from functools import lru_cache
from pathlib import Path

try:
    from .config import resolve_config_url
except ImportError:  # running this file directly rather than as a package module
    from config import resolve_config_url


SECRETS_ENV = "ELV_SECRETS"
TOKEN_FILE_ENV = "ELV_TOKEN_FILE"
ELV_BIN_ENV = "ELV_BIN"
TOKEN_SEARCH_PATHS = (Path("token.txt"),)
MODE_FLAGS = {
    "state-channel": ["--state-channel"],
    "reenc": ["--reenc"],
    "update": ["--update"],
}
MODE_HELP = {
    "state-channel": "off-chain read token: metadata, playout, parts (default)",
    "reenc": "read with re-encryption, for encrypted content",
    "update": "write token; an on-chain transaction, so it costs gas",
}
DEFAULT_MODE = "state-channel"
TIMEOUT = 180
RETRIES = 3
ERROR_LIMIT = 2000

# `elv` reports failures as a `cause:`-linked chain of `op [...] kind [...]`
# links, and the last link names the real problem. A denial from the fabric or
# from the content contract is final -- the key simply has no grant -- so it is
# worth telling apart from a transient error.
PERMISSION_MARKERS = ("permission denied", "access denied", "not authorized")
_STACK_FRAME = re.compile(r"^\s+(?:github\.com/|[\w./-]+\.go:\d+)")
_BANNER = re.compile(r"^.*?ERR!\s+command failed\s+(?:command=\S+\s+)?"
                     r"(?:version=\S+\s+)?(?:error=)?")


def elv_binary() -> str:
    found = os.environ.get(ELV_BIN_ENV) or shutil.which("elv") or Path.home() / "bin" / "elv"
    if not Path(found).exists():
        raise RuntimeError(
            f"elv CLI not found; looked at ${ELV_BIN_ENV}, on PATH and in ~/bin"
        )
    return str(found)


def auth_flags(secret: str | None = None, token: str | None = None) -> list[str]:
    """CLI flags identifying the caller. A secret can sign new authorizations
    and decrypt parts; a bare token can only spend the access it already has."""
    if secret:
        return ["--secret", secret]
    if token:
        return ["--token", token]
    raise ValueError("need either a signing secret or an auth token")


def elv_error(result: subprocess.CompletedProcess) -> str:
    """The useful part of an `elv` failure: its op/kind chain and root cause.

    The CLI prints a banner, then the cause chain, then a Go stack trace.
    Head-truncating the lot cuts inside the banner and drops the one line that
    says why the command failed, so keep the chain and discard the trace.
    """
    text = (result.stderr or "").strip() or (result.stdout or "").strip()
    links = []
    for line in text.splitlines():
        if _STACK_FRAME.match(line):
            break
        link = _BANNER.sub("", line).strip()
        if link:
            links.append(link)
    return " ".join(links)[:ERROR_LIMIT]


def is_permission_error(message: str) -> bool:
    """Whether `message` is a refusal rather than a failure worth retrying."""
    lowered = message.lower()
    return any(marker in lowered for marker in PERMISSION_MARKERS)


def run_elv(arguments: list[str], config_url: str | None = None,
            timeout: int = TIMEOUT, retries: int = 1):
    """Run an `elv` subcommand and return its parsed JSON output.

    With *retries* above 1, a non-zero exit or unparseable output is retried
    with a linear backoff; the last error is what surfaces if all attempts fail.
    A permission denial is final and is not retried.
    """
    command = [elv_binary(), *arguments, "--config-url", resolve_config_url(config_url)]
    last_error = None
    for attempt in range(retries):
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            last_error = elv_error(result)
            if is_permission_error(last_error):
                # The chain has answered; asking again changes nothing.
                break
        else:
            try:
                return json.loads(result.stdout)
            except ValueError:
                last_error = f"unexpected elv output: {result.stdout[:200]}"
        if attempt + 1 < retries:
            time.sleep(0.5 * (attempt + 1))
    raise RuntimeError(f"elv {' '.join(arguments[:3])} failed: {last_error}")


def find_secret(secret: str | None = None) -> str | None:
    """The hex signing key: the argument, else $ELV_SECRETS, else None."""
    return (secret or os.environ.get(SECRETS_ENV) or "").strip() or None


def find_token_file(path=None) -> Path:
    candidates = [Path(path)] if path else []
    if not path and os.environ.get(TOKEN_FILE_ENV):
        candidates.append(Path(os.environ[TOKEN_FILE_ENV]))
    candidates += list(TOKEN_SEARCH_PATHS)
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "no token file found; looked at "
        + ", ".join(str(candidate) for candidate in candidates)
    )


def load_token(path=None) -> str:
    """Read a token from its file. The token is the last non-empty line, so a
    file that also records how it was minted still works."""
    resolved = find_token_file(path)
    lines = [line.strip() for line in resolved.read_text().splitlines() if line.strip()]
    if not lines:
        raise ValueError(f"{resolved} contains no token")
    return lines[-1]


def resolve_token(value=None) -> str:
    """Accept a token, a path to a token file, or nothing (./token.txt)."""
    if value and not Path(value).exists():
        return value
    return load_token(value)


@lru_cache(maxsize=4096)
def create_token(
    secret: str,
    qid: str,
    mode: str = DEFAULT_MODE,
    library_id: str | None = None,
    config_url: str | None = None,
) -> str:
    """Return the bearer token for `qid`, signed with `secret`."""
    if mode not in MODE_FLAGS:
        raise ValueError(f"unknown mode {mode!r}; expected one of {sorted(MODE_FLAGS)}")

    arguments = ["content", "token", "create", qid, *MODE_FLAGS[mode],
                 "--secret", secret]
    if library_id:
        arguments += ["--library", library_id]

    payload = run_elv(arguments, config_url, retries=RETRIES)
    token = payload.get("bearer")
    if not token:
        raise RuntimeError(f"no bearer token from elv for {qid} ({mode})")
    return token


def add_arguments(parser):
    parser.add_argument("qid", help="content object id")
    parser.add_argument("--secret", help=f"hex signing key (default: ${SECRETS_ENV})")
    modes = parser.add_mutually_exclusive_group()
    for mode in MODE_FLAGS:
        modes.add_argument(f"--{mode}", dest="mode", action="store_const",
                           const=mode, help=MODE_HELP[mode])
    parser.set_defaults(mode=DEFAULT_MODE)
    parser.add_argument("--library", help="library id, if the CLI cannot resolve it")
    parser.add_argument("--config-url", help="fabric config url")
    return parser


def run(args):
    secret = find_secret(args.secret)
    if not secret:
        raise SystemExit(f"no signing key; pass --secret or set ${SECRETS_ENV}")
    print(create_token(secret, args.qid, args.mode, args.library, args.config_url))


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Create a fabric token via elv.")
    run(add_arguments(parser).parse_args())


if __name__ == "__main__":
    main()
