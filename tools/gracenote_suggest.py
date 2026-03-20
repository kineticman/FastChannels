#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import create_app
from app.gracenote_suggest import SuggestionChannel, suggest_gracenote_matches
from app.models import AppSettings, Channel


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Query Channels DVR /tms/stations and rank Gracenote matches.")
    parser.add_argument("query", nargs="?", help="Raw search term, e.g. 'Tony Robbins Network'")
    parser.add_argument("--channel-id", type=int, help="FastChannels channel id to suggest against")
    parser.add_argument("--limit", type=int, default=10, help="Max results to print")
    return parser


def main() -> int:
    args = _build_parser().parse_args()

    app = create_app()
    with app.app_context():
        settings = AppSettings.get()
        dvr_url = (settings.effective_channels_dvr_url() or "").strip()
        if not dvr_url:
            print("Channels DVR URL is not configured.", file=sys.stderr)
            return 2

        channel = None
        query = (args.query or "").strip()
        if args.channel_id:
            ch = Channel.query.get(args.channel_id)
            if ch is None:
                print(f"Channel not found: {args.channel_id}", file=sys.stderr)
                return 2
            channel = SuggestionChannel(
                id=ch.id,
                name=ch.name,
                source_name=ch.source.name if ch.source else None,
                country=ch.country,
                language=ch.language,
                category=ch.category,
                gracenote_id=ch.gracenote_id,
            )
            if not query:
                query = ch.name

        if not channel and not query:
            print("Provide either a query or --channel-id.", file=sys.stderr)
            return 2

        payload = suggest_gracenote_matches(
            dvr_url,
            channel=channel,
            query=query,
            limit=max(1, min(args.limit, 25)),
        )
        print(json.dumps(payload, indent=2))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
