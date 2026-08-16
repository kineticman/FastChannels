from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass
import json
from typing import Any
from urllib.parse import quote

import requests

log = logging.getLogger(__name__)

_NAME_CLEAN_RE = re.compile(r"[^a-z0-9]+")
_DROP_WORDS = {
    "channel",
    "tv",
    "network",
    "hd",
    "uhd",
    "4k",
    "live",
}
_TRAILING_CALL_SIGN_RE = re.compile(r"\(([A-Za-z0-9-]{2,10})\)\s*$")
# Feed-variant suffixes TVE scrapers append in the same trailing-parens spot
# as a real call sign (NBC/Warner TVE east/west feeds, generic quality tags)
# — must not be treated as a call sign or they poison the exact-match scoring.
_NON_CALL_SIGN_SUFFIXES = {"EAST", "WEST", "HD", "SD", "UHD", "4K"}


def _extract_call_sign(name: str | None) -> str | None:
    """Pull a trailing '(CALLSIGN)' out of a channel name, e.g. TVE scrapers like
    Cox format channels as 'A&E (AENHD)'. Returns the call sign uppercased."""
    match = _TRAILING_CALL_SIGN_RE.search((name or "").strip())
    if not match:
        return None
    call_sign = match.group(1).upper()
    return None if call_sign in _NON_CALL_SIGN_SUFFIXES else call_sign


def _normalize_name(value: str | None, *, strip_generic_words: bool = False) -> str:
    raw = unicodedata.normalize("NFKD", value or "")
    raw = "".join(ch for ch in raw if not unicodedata.combining(ch))
    raw = raw.casefold().replace("&", " and ")
    raw = _NAME_CLEAN_RE.sub(" ", raw).strip()
    if not raw:
        return ""
    parts = raw.split()
    if strip_generic_words:
        parts = [part for part in parts if part not in _DROP_WORDS]
    return " ".join(parts).strip()


def _token_set(value: str | None, *, strip_generic_words: bool = False) -> set[str]:
    normalized = _normalize_name(value, strip_generic_words=strip_generic_words)
    return {part for part in normalized.split() if part}


def _search_variants(term: str) -> list[str]:
    raw = (term or "").strip()
    if not raw:
        return []
    variants: list[str] = []
    seen: set[str] = set()
    for candidate in (
        raw,
        re.sub(r"\s*&\s*", " and ", raw),
        _normalize_name(raw),
        _normalize_name(raw, strip_generic_words=True),
    ):
        candidate = (candidate or "").strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        variants.append(candidate)
    return variants


def _fetch_station_candidates_once(base: str, term: str, *, timeout: float) -> list[dict[str, Any]]:
    url = f"{base}/tms/stations/{quote(term, safe='')}"
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, list):
        return payload
    return []


@dataclass(slots=True)
class SuggestionChannel:
    id: int | None
    name: str
    source_name: str | None = None
    country: str | None = None
    language: str | None = None
    category: str | None = None
    gracenote_id: str | None = None


def fetch_tms_station_candidates(dvr_url: str, query: str, *, timeout: float = 8.0) -> list[dict[str, Any]]:
    base = (dvr_url or "").strip().rstrip("/")
    term = (query or "").strip()
    if not base:
        raise ValueError("Channels DVR URL is not configured.")
    if not term:
        return []

    # Try every search variant and merge results (deduped by stationId) rather
    # than stopping at the first one that doesn't raise. Confirmed live: a raw
    # name containing '&' (e.g. Cox's "A&E") trips a Gracenote-side error that
    # comes back as HTTP 200 with an {"error": ...} body — which silently
    # decodes to an empty list, not an exception — so without this, the first
    # (worst) variant "succeeding" with zero results would end the search
    # before the cleaned-up variants that actually return matches ever run.
    combined: dict[str, dict[str, Any]] = {}
    last_error: Exception | None = None
    got_any_response = False
    for variant in _search_variants(term):
        try:
            results = _fetch_station_candidates_once(base, variant, timeout=timeout)
        except requests.HTTPError as exc:
            last_error = exc
            status = exc.response.status_code if exc.response is not None else "error"
            if status == 503:
                continue
            raise ValueError(f"Channels DVR Gracenote search failed ({status}).") from exc
        except requests.RequestException as exc:
            last_error = exc
            raise ValueError(f"Channels DVR Gracenote search failed: {exc}") from exc
        except (ValueError, json.JSONDecodeError) as exc:
            last_error = exc
            continue
        got_any_response = True
        for candidate in results:
            station_id = candidate.get("stationId")
            key = str(station_id) if station_id else f"{candidate.get('callSign')}|{candidate.get('name')}"
            combined.setdefault(key, candidate)

    if got_any_response or combined:
        return list(combined.values())

    if isinstance(last_error, requests.HTTPError):
        status = last_error.response.status_code if last_error.response is not None else "error"
        raise ValueError(f"Channels DVR Gracenote search failed ({status}).") from last_error
    if isinstance(last_error, requests.RequestException):
        raise ValueError(f"Channels DVR Gracenote search failed: {last_error}") from last_error
    raise ValueError("Channels DVR Gracenote search returned invalid data.")


def _score_candidate(channel: SuggestionChannel, candidate: dict[str, Any]) -> tuple[int, list[str]]:
    reasons: list[str] = []
    score = 0

    query_name = channel.name or ""
    cand_name = str(candidate.get("name") or "").strip()
    cand_type = str(candidate.get("type") or "").strip()
    cand_langs = candidate.get("bcastLangs") or []

    q_norm = _normalize_name(query_name)
    c_norm = _normalize_name(cand_name)
    q_soft = _normalize_name(query_name, strip_generic_words=True)
    c_soft = _normalize_name(cand_name, strip_generic_words=True)
    q_tokens = _token_set(query_name, strip_generic_words=True)
    c_tokens = _token_set(cand_name, strip_generic_words=True)

    if q_norm and q_norm == c_norm:
        score += 120
        reasons.append("exact normalized name")
    elif q_soft and q_soft == c_soft:
        score += 95
        reasons.append("exact core name")
    elif q_norm and c_norm and (q_norm in c_norm or c_norm in q_norm):
        score += 40
        reasons.append("substring name match")

    if q_tokens and c_tokens:
        shared = q_tokens & c_tokens
        if shared:
            overlap = len(shared) / max(len(q_tokens), len(c_tokens))
            bonus = int(overlap * 40)
            if bonus > 0:
                score += bonus
                reasons.append(f"token overlap {len(shared)}/{max(len(q_tokens), len(c_tokens))}")
        elif q_norm and c_norm:
            score -= 20

    if cand_type.casefold() == "streaming":
        score += 8
        reasons.append("streaming type")

    # Prefer HD over SD when candidates are otherwise a name-match tie —
    # confirmed live that Gracenote/TMS commonly lists an SDTV entry ahead of
    # the matching HDTV one for the same network (e.g. "TNT" SD vs "TNTHDB"
    # HD both matching a "TNT (East)" query equally on name), and the stable
    # sort was letting the SD entry win by API-order alone.
    video_type = str((candidate.get("videoQuality") or {}).get("videoType") or "").strip().upper()
    if video_type == "HDTV":
        score += 6
        reasons.append("HD")
    elif video_type == "UHDTV":
        score += 4
        reasons.append("4K")
    elif video_type == "SDTV":
        score -= 2

    channel_lang = (channel.language or "").strip().casefold()
    if channel_lang and cand_langs:
        for lang in cand_langs:
            lang_norm = str(lang or "").strip().casefold()
            if lang_norm == channel_lang or lang_norm.startswith(f"{channel_lang}-"):
                score += 5
                reasons.append(f"language {lang}")
                break

    if candidate.get("affiliateCallSign"):
        score += 1

    cand_call_sign = str(candidate.get("callSign") or "").strip().upper()
    if cand_call_sign:
        score += 1
        query_call_sign = _extract_call_sign(query_name)
        if query_call_sign:
            if cand_call_sign == f"{query_call_sign}DT":
                # Local broadcast affiliates: TVE scrapers report the bare
                # over-the-air call sign (e.g. "WCMH"), but that bare call
                # sign is Gracenote's legacy/analog placeholder record — the
                # actual HD digital station is registered under the same
                # call sign plus a "DT" suffix ("WCMHDT"). Confirmed live via
                # /tms/stations/WCMH: WCMH itself has no videoType (Analog),
                # while WCMHDT is the HDTV entry. Prefer it over an exact
                # bare match.
                score += 210
                reasons.append(f"call sign match, HD affiliate ({cand_call_sign})")
            elif query_call_sign == cand_call_sign:
                score += 130
                reasons.append(f"call sign match ({cand_call_sign})")

    return score, reasons


def classify_confidence(score: int) -> str:
    if score >= 110:
        return "high"
    if score >= 70:
        return "medium"
    if score >= 35:
        return "low"
    return "weak"


def suggest_gracenote_matches(
    dvr_url: str,
    *,
    channel: SuggestionChannel | None = None,
    query: str | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    if channel is None:
        raw_query = (query or "").strip()
        if not raw_query:
            raise ValueError("A channel or query is required.")
        channel = SuggestionChannel(id=None, name=raw_query)
    else:
        raw_query = (query or channel.name or "").strip()
        if not raw_query:
            raise ValueError("Channel name is empty.")

    results = fetch_tms_station_candidates(dvr_url, raw_query)
    ranked: list[dict[str, Any]] = []
    for candidate in results:
        score, reasons = _score_candidate(channel, candidate)
        ranked.append(
            {
                "score": score,
                "confidence": classify_confidence(score),
                "reasons": reasons,
                "station_id": candidate.get("stationId"),
                "name": candidate.get("name"),
                "affiliate": candidate.get("affiliateCallSign") or "",
                "type": candidate.get("type") or "",
                "video": ((candidate.get("videoQuality") or {}).get("videoType") or ""),
                "primary_language": ((candidate.get("bcastLangs") or [""])[0] or ""),
                "call_sign": candidate.get("callSign") or "",
                "logo_url": ((candidate.get("preferredImage") or {}).get("uri") or ""),
            }
        )

    ranked.sort(
        key=lambda item: (
            item["score"],
            bool(item["station_id"]),
            len(_normalize_name(item["name"])),
        ),
        reverse=True,
    )

    return {
        "query": raw_query,
        "candidate_count": len(ranked),
        "results": ranked[: max(1, int(limit or 10))],
    }
