#!/usr/bin/env python3

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
# A denial caused by a duplicate pending access request: concurrent part reads
# with one key each ask the content contract for access, and the chain reports
# all but one as "already known". The grant lands moments later, so retry.
TRANSIENT_ACCESS_MARKERS = ("already known", "waittransact")
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


def is_transient_access_error(message: str) -> bool:
    """A permission error caused by a concurrent access request still landing
    on-chain (see TRANSIENT_ACCESS_MARKERS), which succeeds on retry."""
    lowered = message.lower()
    return any(marker in lowered for marker in TRANSIENT_ACCESS_MARKERS)


def is_permission_error(message: str) -> bool:
    """Whether `message` is a refusal rather than a failure worth retrying.

    The access-request race (is_transient_access_error) also reads
    "permission denied", but is excluded: it succeeds on retry.
    """
    lowered = message.lower()
    return (any(marker in lowered for marker in PERMISSION_MARKERS)
            and not is_transient_access_error(message))


def run_elv(arguments: list[str], config_url: str | None = None, timeout: int = TIMEOUT):
    """Run an `elv` subcommand and return its parsed JSON output."""
    command = [elv_binary(), *arguments, "--config-url", resolve_config_url(config_url)]
    result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(
            f"elv {' '.join(arguments[:3])} failed: {elv_error(result)}"
        )
    try:
        return json.loads(result.stdout)
    except ValueError:
        raise RuntimeError(f"unexpected elv output: {result.stdout[:200]}")


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

    command = [elv_binary(), "content", "token", "create", qid]
    command += MODE_FLAGS[mode]
    command += ["--secret", secret, "--config-url", resolve_config_url(config_url)]
    if library_id:
        command += ["--library", library_id]

    last_error = None
    for attempt in range(RETRIES):
        result = subprocess.run(
            command, capture_output=True, text=True, timeout=TIMEOUT
        )
        if result.returncode != 0:
            last_error = elv_error(result)
            if is_permission_error(last_error):
                # The chain has answered; asking twice more changes nothing.
                break
            time.sleep(0.5 * (attempt + 1))
            continue
        try:
            payload = json.loads(result.stdout)
        except ValueError:
            last_error = f"unexpected elv output: {result.stdout[:200]}"
            time.sleep(0.5 * (attempt + 1))
            continue
        token = payload.get("bearer")
        if token:
            return token
        last_error = "no bearer token in elv output"

    raise RuntimeError(f"elv token create failed for {qid} ({mode}): {last_error}")


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
