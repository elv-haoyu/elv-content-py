"""
CLI entry point for elv_title_extractor.

Usage:
  python -m elv_title_extractor title --token TOKEN --qids iq__xxx iq__yyy -o output.json
  python -m elv_title_extractor download --token TOKEN --qid iq__xxx --start 0 --end 120000
"""

import argparse
import json
import sys

from .extractor import TitleExtractor
from .downloader import ContentDownloader


def _resolve_token(raw: str) -> str:
    """Resolve a token string or file path to a token value."""
    try:
        with open(raw, "r") as f:
            return f.readlines()[-1].strip()
    except (FileNotFoundError, IsADirectoryError):
        return raw


def cmd_title(args):
    token = _resolve_token(args.token)
    kwargs = {"auth_token": token}
    if args.config_url:
        kwargs["config_url"] = args.config_url

    extractor = TitleExtractor(**kwargs)
    results = extractor.extract_batch(args.qids)

    if args.output:
        TitleExtractor.save(results, args.output)
        print(f"Saved to {args.output}", file=sys.stderr)
    else:
        print(json.dumps(results, indent=2))


def cmd_download(args):
    token = _resolve_token(args.token)
    kwargs = {"auth_token": token}
    if args.config_url:
        kwargs["config_url"] = args.config_url

    downloader = ContentDownloader(**kwargs)
    downloader.download(
        content_id=args.qid,
        start_ms=args.start,
        end_ms=args.end,
        output_dir=args.output_dir,
        offering=args.offering,
        format=args.format,
        representation=args.representation,
        audio=args.audio,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Eluvio content fabric utilities"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ---- title subcommand ------------------------------------------------ #
    tp = sub.add_parser("title", help="Extract title metadata")
    tp.add_argument("--token", required=True,
                    help="Auth token or path to token file")
    tp.add_argument("--qids", nargs="+", required=True,
                    help="Content object IDs")
    tp.add_argument("--config-url", default=None, help="Fabric config URL")
    tp.add_argument("-o", "--output", default=None,
                    help="Output JSON file path")
    tp.set_defaults(func=cmd_title)

    # ---- download subcommand --------------------------------------------- #
    dp = sub.add_parser("download", help="Download a video segment")
    dp.add_argument("--token", required=True,
                    help="Auth token or path to token file")
    dp.add_argument("--qid", required=True, help="Content object ID (iq__...)")
    dp.add_argument("--start", type=int, required=True,
                    metavar="MS", help="Start time in ms")
    dp.add_argument("--end", type=int, required=True,
                    metavar="MS", help="End time in ms")
    dp.add_argument("--output-dir", default="downloads",
                    help="Output directory")
    dp.add_argument("--offering", default="default_clear",
                    help="Playout offering")
    dp.add_argument("--format", default="mp4", help="Container format")
    dp.add_argument("--representation", default=None,
                    help="Video representation string")
    dp.add_argument("--audio", default=None, help="Audio track identifier")
    dp.add_argument("--config-url", default=None, help="Fabric config URL")
    dp.set_defaults(func=cmd_download)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
