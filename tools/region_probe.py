#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.scrapers.pluto import ALLOWED_COUNTRY_CODES as PLUTO_ALLOWED
from app.scrapers.distro import DistroScraper
from app.scrapers.pluto import PlutoScraper
from app.scrapers.samsung import SamsungScraper


@dataclass(slots=True)
class ProbeRow:
    region: str
    source_channel_id: str
    name: str
    country: str
    category: str | None
    number: int | None


def _probe_pluto(regions: list[str]) -> list[ProbeRow]:
    scraper = PlutoScraper({"country_codes": ",".join(regions)})
    rows: list[ProbeRow] = []
    for region in regions:
        channels = scraper._fetch_country_channels(region)
        for ch in channels:
            rows.append(
                ProbeRow(
                    region=region,
                    source_channel_id=ch.source_channel_id,
                    name=ch.name,
                    country=ch.country,
                    category=ch.category,
                    number=ch.number,
                )
            )
    return rows


def _probe_samsung(regions: list[str]) -> list[ProbeRow]:
    scraper = SamsungScraper({"region": ",".join(regions)})
    rows: list[ProbeRow] = []
    channels = scraper.fetch_channels()
    by_region = set(r.lower() for r in regions)
    for ch in channels:
        region = (ch.country or "").lower()
        if region and region.lower() not in by_region:
            continue
        rows.append(
            ProbeRow(
                region=region or "?",
                source_channel_id=ch.source_channel_id,
                name=ch.name,
                country=ch.country,
                category=ch.category,
                number=ch.number,
            )
        )
    return rows


def _probe_distro(regions: list[str]) -> list[ProbeRow]:
    scraper = DistroScraper({"geo": ",".join(regions)})
    rows: list[ProbeRow] = []
    channels = scraper.fetch_channels()
    by_region = {r.upper() for r in regions}
    for ch in channels:
        region = (ch.country or "").upper()
        if region and region not in by_region:
            continue
        rows.append(
            ProbeRow(
                region=region or "?",
                source_channel_id=ch.source_channel_id,
                name=ch.name,
                country=ch.country,
                category=ch.category,
                number=ch.number,
            )
        )
    return rows


def _summarize(rows: list[ProbeRow], *, limit: int) -> str:
    by_region: dict[str, list[ProbeRow]] = defaultdict(list)
    by_name: dict[str, list[ProbeRow]] = defaultdict(list)
    by_id: dict[str, list[ProbeRow]] = defaultdict(list)
    for row in rows:
        by_region[row.region].append(row)
        by_name[row.name].append(row)
        by_id[row.source_channel_id].append(row)

    lines: list[str] = []
    lines.append("Per-region counts:")
    for region in sorted(by_region):
        lines.append(f"  {region}: {len(by_region[region])}")

    multi_name = []
    for name, vals in by_name.items():
        regions = sorted({v.region for v in vals})
        if len(regions) > 1:
            multi_name.append((name, regions, vals))
    multi_name.sort(key=lambda item: (-len(item[1]), item[0].casefold()))

    same_id = []
    for source_channel_id, vals in by_id.items():
        regions = sorted({v.region for v in vals})
        if len(regions) > 1:
            same_id.append((source_channel_id, regions, vals))
    same_id.sort(key=lambda item: (-len(item[1]), item[0]))

    lines.append("")
    lines.append(f"Same-name collisions across regions: {len(multi_name)}")
    for name, regions, vals in multi_name[:limit]:
        cat_preview = sorted({v.category or "—" for v in vals})
        lines.append(f"  {name} -> {', '.join(regions)} | categories: {', '.join(cat_preview[:3])}")

    lines.append("")
    lines.append(f"Same source_channel_id across regions: {len(same_id)}")
    for source_channel_id, regions, vals in same_id[:limit]:
        names = sorted({v.name for v in vals})
        lines.append(f"  {source_channel_id} -> {', '.join(regions)} | names: {', '.join(names[:3])}")

    return "\n".join(lines)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe multi-region channel overlap for a provider.")
    parser.add_argument("provider", choices=["pluto", "samsung", "distro"])
    parser.add_argument(
        "--regions",
        required=True,
        help="Comma-separated region list, e.g. us_east,ca,uk or us,ca,gb or us,ca,mx",
    )
    parser.add_argument("--limit", type=int, default=40, help="How many overlap examples to print.")
    return parser.parse_args()


def _validate_regions(provider: str, regions: Iterable[str]) -> list[str]:
    cleaned = [r.strip() for r in regions if r.strip()]
    if provider == "pluto":
        bad = [r for r in cleaned if r not in PLUTO_ALLOWED]
        if bad:
            raise SystemExit(f"Unsupported Pluto region(s): {', '.join(bad)}. Allowed: {', '.join(PLUTO_ALLOWED)}")
        return cleaned
    if provider == "distro":
        return [r.upper() for r in cleaned]
    return [r.lower() for r in cleaned]


def main() -> int:
    args = _parse_args()
    regions = _validate_regions(args.provider, args.regions.split(","))
    if args.provider == "pluto":
        rows = _probe_pluto(regions)
    elif args.provider == "distro":
        rows = _probe_distro(regions)
    else:
        rows = _probe_samsung(regions)

    print(f"Provider: {args.provider}")
    print(f"Regions: {', '.join(regions)}")
    print(f"Fetched rows: {len(rows)}")
    print("")
    print(_summarize(rows, limit=args.limit))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
