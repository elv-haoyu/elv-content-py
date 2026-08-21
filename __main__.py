"""
CLI entry point.

Usage:
  python -m elv_content_py token iq__xxx --secret 0x<hex>
  python -m elv_content_py parts iq__xxx --secret 0x<hex> -o /ml/data/content
  python -m elv_content_py title --qids iq__xxx iq__yyy -o output.json
  python -m elv_content_py download --qid iq__xxx --start 0 --end 120000

`token` and `parts` speak to the `elv` CLI and need only requests; `title` and
`download` go through elv_client_py. Both read their auth token from --token,
which takes a token or a file whose last line is one, and defaults to
./token.txt.
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from . import elv_token, parts
from .elv_token import resolve_token


def cmd_token(args):
    elv_token.run(args)


def cmd_parts(args):
    parts.configure_logging(args.verbose)
    sys.exit(parts.run(args))


def cmd_title(args):
    from .extractor import TitleExtractor

    token = resolve_token(args.token)
    kwargs = {"auth_token": token, "metadata_dir": Path(args.metadata_dir)}
    if args.config_url:
        kwargs["config_url"] = args.config_url

    extractor = TitleExtractor(**kwargs)
    results = extractor.extract_batch(args.qids)

    if args.output:
        # extract_batch already cached one file per QID under metadata_dir;
        # -o additionally writes the combined result to a single file.
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Saved to {args.output}", file=sys.stderr)
    else:
        print(json.dumps(results, indent=2))


def cmd_download(args):
    from .downloader import ContentDownloader

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    token = resolve_token(args.token)
    kwargs = {"auth_token": token}
    if args.config_url:
        kwargs["config_url"] = args.config_url

    downloader = ContentDownloader(**kwargs)
    if args.list_reps:
        for rep in downloader.representations(args.qid, args.offering):
            print(rep)
        return
    path = downloader.download(
        content_id=args.qid,
        start_ms=args.start,
        end_ms=args.end,
        output_dir=args.output_dir,
        offering=args.offering,
        format=args.format,
        audio_only=args.audio_only,
        representation=args.representation,
    )
    if path is None:
        sys.exit(1)
    print(path)


def main():
    parser = argparse.ArgumentParser(
        description="Eluvio content fabric utilities"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ---- token subcommand ------------------------------------------------ #
    kp = sub.add_parser("token", help="Create a fabric auth token via the elv CLI")
    elv_token.add_arguments(kp)
    kp.set_defaults(func=cmd_token)

    # ---- parts subcommand ------------------------------------------------ #
    pp = sub.add_parser(
        "parts", help="Download a whole object's media parts via the elv CLI"
    )
    parts.add_arguments(pp)
    pp.set_defaults(func=cmd_parts)

    # ---- title subcommand ------------------------------------------------ #
    tp = sub.add_parser("title", help="Extract title metadata")
    tp.add_argument("--token", default=None,
                    help="Auth token, or a file whose last line is one "
                         "(default: ./token.txt)")
    tp.add_argument("--qids", nargs="+", required=True,
                    help="Content object IDs")
    tp.add_argument("--config-url", default=None, help="Fabric config URL")
    tp.add_argument("--metadata-dir", default="metadata",
                    help="Directory for per-QID title caches (default: metadata)")
    tp.add_argument("-o", "--output", default=None,
                    help="Output JSON file path")
    tp.set_defaults(func=cmd_title)

    # ---- download subcommand --------------------------------------------- #
    dp = sub.add_parser("download", help="Download a transcoded time range")
    dp.add_argument("--token", default=None,
                    help="Auth token, or a file whose last line is one "
                         "(default: ./token.txt)")
    dp.add_argument("--qid", required=True, help="Content object ID (iq__...)")
    dp.add_argument("--start", type=int, default=0,
                    metavar="MS", help="Start time in ms")
    dp.add_argument("--end", type=int, default=0,
                    metavar="MS", help="End time in ms")
    dp.add_argument("--output-dir", default="downloads",
                    help="Output directory (default: downloads)")
    dp.add_argument("--offering", default="default_clear",
                    help="Playout offering (default: default_clear)")
    dp.add_argument("--format", default="mp4",
                    help="Container format (default: mp4)")
    dp.add_argument("--audio-only", action="store_true",
                    help="Skip the video representation")
    dp.add_argument("--representation", default=None,
                    help="Video rendition: an id from --list-reps, or "
                         "lowest/highest (default: try all, lowest first)")
    dp.add_argument("--list-reps", action="store_true",
                    help="Print the available video renditions and exit")
    dp.add_argument("--config-url", default=None, help="Fabric config URL")
    dp.set_defaults(func=cmd_download)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
