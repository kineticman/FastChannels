"""
directv_numbers.py — DirecTV's own channel numbers, with subchannel suffixes.

DirecTV's AllChannels lineup reuses one channel number for several feeds:
"MLB Network" and "MLB Network Alternate" are both 213, "ION East" shares 305
with "WNBA on ION 1/2/3", and regional sports overflow feeds pile onto the
network's number. Satellite lineups additionally list an HD row and an SD row
for the same channel under the same number.

This module turns that into one unambiguous number per row so a DVR can
import the lineup as the subscriber sees it in DirecTV's own guide:

  * the main feed keeps the bare number ("305")
  * alternates get a ".N" subchannel suffix ("305.1", "305.2", ...)
  * an SD row that duplicates an HD row is flagged as its twin so the caller
    can collapse the pair

The rules are a port of the jq pipeline in the ah4c DirecTV grabber, which has
been tuned against real lineups. They operate only on the rows the caller
passes in, so they adapt to whatever package a given account has — nothing
here assumes any particular channel exists.

Pure functions, no I/O: easy to run against a saved AllChannels capture.
"""
from __future__ import annotations

import re
from collections import OrderedDict
from dataclasses import dataclass, field

_HD_WORD_RE = re.compile(r'\bHD\b', re.IGNORECASE)
_HD_CALLSIGN_RE = re.compile(r'HD$', re.IGNORECASE)
_ALTERNATE_RE = re.compile(r'\bAlternate\b', re.IGNORECASE)
_TRAILING_NUMBER_RE = re.compile(r'(\d+)\s*$')
_SPACES_RE = re.compile(r' +')


@dataclass
class NumberedRow:
    """One AllChannels row after numbering."""
    ccid: str
    provider_number: str | None      # "305" / "305.1" — None when the row has no usable number
    gracenote_id: str | None         # externalListingId, possibly borrowed from an HD/SD twin
    sd_twin_of: str | None = None    # ccid of the HD row this SD row duplicates
    raw: dict = field(default_factory=dict, repr=False)


def _s(row: dict, *keys: str) -> str:
    for k in keys:
        v = row.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ''


def _name(row: dict) -> str:
    return _s(row, 'channelName', 'name', 'displayName', 'title')


def _call_sign(row: dict) -> str:
    return _s(row, 'callSign', 'callsign')


def _ccid(row: dict) -> str:
    return _s(row, 'ccid', 'ccId', 'channelId', 'channel_id', 'id')


def _channel_number(row: dict) -> int | None:
    raw = _s(row, 'channelNumber', 'channel_number', 'number')
    return int(raw) if raw.isdigit() else None


def _is_hd(row: dict) -> bool:
    return bool(_HD_WORD_RE.search(_name(row))) or bool(_HD_CALLSIGN_RE.search(_call_sign(row)))


def _name_base(row: dict) -> str:
    base = _HD_WORD_RE.sub('', _name(row))
    return _SPACES_RE.sub(' ', base).strip().lower()


def _call_sign_base(row: dict) -> str:
    cs = _call_sign(row)
    if _HD_CALLSIGN_RE.search(cs):
        cs = cs[:-2]
    return cs.lower()


def real_station_id(row: dict) -> str | None:
    """externalListingId when it is a real Gracenote/TMS station id.

    DirecTV often has not back-filled a real mapping onto a row and just echoes
    the row's own ccid (a short internal id) instead. Real station ids are
    5+ digits, so anything shorter — or equal to the ccid — is treated as
    "no id".
    """
    ext = _s(row, 'externalListingId')
    if not ext or ext == _ccid(row) or not ext.isdigit() or len(ext) < 5:
        return None
    return ext


def _is_alternate(row: dict) -> bool:
    return bool(_ALTERNATE_RE.search(_name(row)))


def _alt_number(row: dict, base_number: int) -> int | None:
    """Trailing number in the name ("WNBA on ION 2" -> 2), or None.

    A trailing number that is the channel's own number ("MSG Sportsnet HD 635"
    on channel 635) is part of the branding, not an alternate index.
    """
    if _is_alternate(row):
        return None
    m = _TRAILING_NUMBER_RE.search(_name(row))
    if not m:
        return None
    n = int(m.group(1))
    # "... 0" is not an alternate index either; suffixes start at .1.
    if n == base_number or n < 1:
        return None
    return n


def _collapse_hd_pairs(group: list[dict]) -> tuple[list[dict], dict[str, str], dict[str, str]]:
    """Pair SD rows with their HD counterpart inside one channel-number group.

    Pairing uses two signals: the channelName with "HD" stripped, then the
    callSign with a trailing "HD" stripped. The name is checked first and,
    when it matches, used exclusively — callSign alone is not reliable, since
    two different channels can share one raw callSign. callSign is the
    fallback for local affiliates whose HD/SD names diverge but whose
    callSign does not.

    Returns (surviving rows, {sd ccid: hd ccid}, {hd ccid: borrowed station id}).
    Anything unpaired (true alternates, distinct feeds like "TNT" vs "TNT
    West") survives untouched for the suffix logic.
    """
    hd_rows = [r for r in group if _is_hd(r)]
    sd_rows = [r for r in group if not _is_hd(r)]
    if not hd_rows or not sd_rows:
        return list(group), {}, {}

    def counterparts(hd: dict) -> list[dict]:
        by_name = [s for s in sd_rows if _name_base(s) == _name_base(hd)]
        if by_name:
            return by_name
        return [s for s in sd_rows if _call_sign_base(s) == _call_sign_base(hd)]

    twins: dict[str, str] = {}
    borrowed: dict[str, str] = {}
    for hd in hd_rows:
        pair = counterparts(hd)
        for sd in pair:
            twins.setdefault(_ccid(sd), _ccid(hd))
        if real_station_id(hd) is None:
            for sd in pair:
                sid = real_station_id(sd)
                if sid:
                    borrowed[_ccid(hd)] = sid
                    break

    survivors = [r for r in group if _ccid(r) not in twins]
    return survivors, twins, borrowed


def _suffix_group(group: list[dict], base_number: int) -> dict[str, str]:
    """Assign "N" / "N.x" numbers within one duplicate channel-number group.

    The one entry that is neither "... Alternate" nor ends in a number is the
    main channel and keeps the bare number. "... Alternate" entries get
    sequential .1/.2 suffixes; "... N" entries (e.g. "WNBA on ION 1") get a
    suffix matching their own trailing number rather than their position in
    the source data. If every entry is numbered and none stands out as a
    plain main ("ESPN+ 1" through "ESPN+ 7"), the lowest-numbered entry is
    the implicit main. Anything that does not resolve to exactly one main
    falls back to order-based suffixing so nothing is silently mishandled.
    """
    base = str(base_number)
    result: dict[str, str] = {}
    if len(group) == 1:
        result[_ccid(group[0])] = base
        return result

    used: set[int] = set()

    def take(n: int) -> str:
        # Bump to the next free suffix if two entries claim the same one.
        while n in used:
            n += 1
        used.add(n)
        return f'{base}.{n}'

    def next_free(start: int = 1) -> int:
        n = start
        while n in used:
            n += 1
        return n

    mains = [r for r in group if not _is_alternate(r) and _alt_number(r, base_number) is None]
    numbered = [r for r in group if _alt_number(r, base_number) is not None]

    if len(mains) == 1:
        result[_ccid(mains[0])] = base
        for r in sorted(numbered, key=lambda r: _alt_number(r, base_number)):
            result[_ccid(r)] = take(_alt_number(r, base_number))
        for r in group:
            if _ccid(r) not in result:
                result[_ccid(r)] = take(next_free())
        return result

    if not mains and len(numbered) == len(group):
        ordered = sorted(group, key=lambda r: _alt_number(r, base_number))
        result[_ccid(ordered[0])] = base
        for r in ordered[1:]:
            result[_ccid(r)] = take(_alt_number(r, base_number))
        return result

    for idx, r in enumerate(group):
        result[_ccid(r)] = base if idx == 0 else take(idx)
    return result


def assign_provider_numbers(rows: list[dict]) -> dict[str, NumberedRow]:
    """Number every row, keyed by ccid. Rows are taken in the order given
    (AllChannels is requested sorted by channel number), which is the
    order-based fallback's tiebreak."""
    groups: OrderedDict[int, list[dict]] = OrderedDict()
    out: dict[str, NumberedRow] = {}
    for row in rows:
        ccid = _ccid(row)
        if not ccid:
            continue
        number = _channel_number(row)
        if number is None:
            out[ccid] = NumberedRow(ccid=ccid, provider_number=None,
                                    gracenote_id=real_station_id(row), raw=row)
            continue
        groups.setdefault(number, []).append(row)

    for number, group in groups.items():
        survivors, twins, borrowed = _collapse_hd_pairs(group)
        numbers = _suffix_group(survivors, number)
        for row in group:
            ccid = _ccid(row)
            if ccid in twins:
                # The SD twin carries DirecTV's number for the pair; the caller
                # decides whether it stays in the lineup at all.
                out[ccid] = NumberedRow(
                    ccid=ccid, provider_number=numbers.get(twins[ccid], str(number)),
                    gracenote_id=real_station_id(row), sd_twin_of=twins[ccid], raw=row,
                )
            else:
                out[ccid] = NumberedRow(
                    ccid=ccid, provider_number=numbers.get(ccid),
                    gracenote_id=borrowed.get(ccid) or real_station_id(row), raw=row,
                )
    return out
