"""
probe_roku_premium.py — Inspect Roku raw content-proxy metadata for premium-ish signals.

This probes a small set of Roku channels and prints the raw fields that may
distinguish free vs premium / MVPD-backed channels before we decide whether
the scraper should filter them upstream.

Run inside container:
  docker exec fastchannels python3 /app/dev/probe_roku_premium.py
  docker exec fastchannels python3 /app/dev/probe_roku_premium.py --names "Tennis+,OAN Plus,NEWSMAX2"
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, "/app")

from app import create_app
from app.models import Source
from app.scrapers.roku import RokuScraper, _EPG_URL


DEFAULT_NAMES = [
    "Tennis+",
    "OAN Plus",
    "NEWSMAX2",
    "BBC News",
    "FOX Weather",
]

FIELDS_TO_SHOW = (
    "businessModel",
    "isUnlocked",
    "price",
    "priceDisplay",
    "license",
    "providerId",
    "providerName",
    "providerType",
    "adsProviderId",
    "mvpdLivefeeds",
    "categories",
    "tags",
    "type",
    "enabled",
    "isAvailable",
    "isFeaturedRowEligible",
)


def _epg_name_to_sid(scraper: RokuScraper, names: list[str]) -> dict[str, str]:
    if not scraper._ensure_session():
        return {}
    response = scraper._api_get(_EPG_URL, timeout=20, label="roku premium probe epg")
    if not response or response.status_code != 200:
        return {}

    wanted = {name.casefold() for name in names}
    mapping: dict[str, str] = {}
    for col in response.json().get("collections", []):
        station = (col.get("features") or {}).get("station") or {}
        title = (station.get("title") or "").casefold()
        sid = (station.get("meta") or {}).get("id")
        if title in wanted and sid:
            mapping[station.get("title") or title] = sid
    return mapping


def _pick_view_option(data: dict) -> dict:
    view_opts = data.get("viewOptions") or [{}]
    return view_opts[0] if view_opts else {}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--names",
        default=",".join(DEFAULT_NAMES),
        help="Comma-separated Roku channel names to probe.",
    )
    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        source = Source.query.filter_by(name="roku").first()
        if not source:
            print("roku source not found")
            return 1

        names = [name.strip() for name in args.names.split(",") if name.strip()]
        scraper = RokuScraper(config=source.config or {})
        sid_map = _epg_name_to_sid(scraper, names)
        print(json.dumps({"requested": names, "mapped": sid_map}, indent=2, sort_keys=True))

        for name in names:
            sid = sid_map.get(name)
            if not sid:
                print(f"\n{name}: no EPG station match")
                continue

            content = scraper._fetch_content(sid)
            if not content:
                print(f"\n{name} ({sid}): content lookup failed")
                continue

            view = _pick_view_option(content)
            summary = {
                "title": content.get("title"),
                "station_id": sid,
                "content_keys": sorted(content.keys()),
                "viewOption_keys": sorted(view.keys()),
            }
            for field in FIELDS_TO_SHOW:
                if field in view:
                    summary[field] = view.get(field)
                elif field in content:
                    summary[field] = content.get(field)
            print(f"\n{name} ({sid})")
            print(json.dumps(summary, indent=2, sort_keys=True, default=str))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
