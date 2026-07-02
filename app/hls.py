import re


# DRM system UUIDs (canonical lowercase form), used to detect ContentProtection /
# KEYFORMAT in both HLS and DASH manifests. Single source of truth — importers must
# lowercase the text they scan before matching.
WIDEVINE_UUID  = 'edef8ba9-79d6-4ace-a3c8-27dcd51d21ed'
PLAYREADY_UUID = '9a04f079-9840-4286-ab92-e65be0885f95'


_VIDEO_CODEC_MAP = {
    'avc1': 'h264', 'avc3': 'h264',
    'hvc1': 'hevc', 'hev1': 'hevc',
    'av01': 'av1',
    'vp09': 'vp9',
}
_FRIENDLY_CODEC_MAP = {
    'avc1': 'H.264', 'avc3': 'H.264',
    'hvc1': 'H.265', 'hev1': 'H.265',
    'mp4a': 'AAC', 'ac-3': 'AC-3', 'ec-3': 'E-AC-3',
    'vp09': 'VP9', 'av01': 'AV1',
}


def _friendly_codecs(raw: str) -> str:
    seen, result = set(), []
    for part in raw.split(','):
        prefix = part.strip().split('.')[0].lower()
        name = _FRIENDLY_CODEC_MAP.get(prefix, prefix)
        if name not in seen:
            seen.add(name)
            result.append(name)
    return '+'.join(result)


# Nominal 16:9 pixel dimensions per frame height, used to label variants that
# advertise BANDWIDTH but no RESOLUTION (e.g. Pluto's ad-stitched masters).
# These are estimates — callers flag them with `resolution_estimated`.
_NOMINAL_DIMS = {
    240: '426x240',  360: '640x360',  480: '854x480',  540: '960x540',
    720: '1280x720', 1080: '1920x1080', 1440: '2560x1440', 2160: '3840x2160',
}

# H.264 bitrate → frame-height ladder (bps, inclusive lower bounds, high → low).
# Rough mid-ladder cutoffs from typical OTT encoding profiles.
_BITRATE_HEIGHT_LADDER = (
    (11_000_000, 2160),
    (6_000_000,  1440),
    (4_000_000,  1080),
    (2_000_000,  720),
    (1_200_000,  540),
    (800_000,    480),
    (450_000,    360),
    (0,          240),
)


def estimate_height_from_bandwidth(bandwidth, video_codec: str = 'unknown') -> int | None:
    """
    Estimate a variant's frame height from its peak BANDWIDTH, for manifests
    that omit RESOLUTION. Tuned for H.264; HEVC/AV1/VP9 fit more pixels per bit,
    so their effective bandwidth is scaled up before the ladder lookup. Returns
    None for missing/zero bandwidth. This is a heuristic — callers should flag
    the result as estimated (never persist it as a measured resolution).
    """
    if not bandwidth or bandwidth <= 0:
        return None
    eff = bandwidth
    if video_codec in ('hevc', 'av1', 'vp9'):
        eff = int(bandwidth * 1.8)
    for floor, height in _BITRATE_HEIGHT_LADDER:
        if eff >= floor:
            return height
    return None


def nominal_resolution(height) -> str | None:
    """Nominal 'WxH' string for an estimated frame height (None if unknown)."""
    return _NOMINAL_DIMS.get(height) if height else None


def parse_stream_info(master_text: str) -> dict | None:
    """
    Parse HLS master playlist variant metadata into a stream_info dict.
    Returns None if the text is not a master playlist (no #EXT-X-STREAM-INF).

    Returned dict keys:
      max_resolution  str   e.g. '3840x2160'  (highest-pixel variant)
      max_width       int | None
      max_height      int | None
      video_codec     str   'h264' | 'hevc' | 'av1' | 'vp9' | 'unknown'
      has_4k          bool  max height >= 2160
      has_hd          bool  max height >= 720
      variants        list  [{resolution?, bandwidth?, codecs?, fps?}]
    """
    if '#EXT-X-STREAM-INF' not in master_text:
        return None

    variants = []
    for line in master_text.splitlines():
        line = line.strip()
        if not line.startswith('#EXT-X-STREAM-INF:'):
            continue
        attrs = line[len('#EXT-X-STREAM-INF:'):]
        v: dict = {}
        m = re.search(r'BANDWIDTH=(\d+)', attrs)
        if m:
            v['bandwidth'] = int(m.group(1))
        m = re.search(r'RESOLUTION=(\d+x\d+)', attrs, re.I)
        if m:
            v['resolution'] = m.group(1)
        m = re.search(r'CODECS="([^"]+)"', attrs)
        if m:
            raw_codecs = m.group(1)
            v['_raw_codecs'] = raw_codecs
            v['codecs'] = _friendly_codecs(raw_codecs)
        m = re.search(r'FRAME-RATE=([\d.]+)', attrs)
        if m:
            v['fps'] = round(float(m.group(1)), 3)
        variants.append(v)

    return _build_stream_info(variants)


def parse_dash_stream_info(mpd_text: str) -> dict | None:
    """Parse a DASH MPD's video Representations into the same stream_info dict shape as
    parse_stream_info (HLS), so DASH sources (Amazon, Sling) get the resolution/codec
    badge too. Returns None if not a parseable MPD with video representations.

    width/height/codecs/frameRate may sit on the <AdaptationSet> or the <Representation>;
    Representation values win, AdaptationSet values are the fallback."""
    if '<MPD' not in mpd_text:
        return None
    import xml.etree.ElementTree as _ET
    try:
        root = _ET.fromstring(mpd_text)
    except Exception:
        return None

    def _local(tag: str) -> str:
        return tag.rsplit('}', 1)[-1]

    def _fps(raw: str | None):
        if not raw:
            return None
        try:
            if '/' in raw:
                num, den = raw.split('/', 1)
                return round(int(num) / int(den), 3)
            return round(float(raw), 3)
        except Exception:
            return None

    variants = []
    for aset in root.iter():
        if _local(aset.tag) != 'AdaptationSet':
            continue
        a_mime, a_ct = aset.get('mimeType', ''), aset.get('contentType', '')
        a_w, a_h = aset.get('width'), aset.get('height')
        a_codecs, a_fr = aset.get('codecs'), aset.get('frameRate')
        for rep in list(aset):
            if _local(rep.tag) != 'Representation':
                continue
            w = rep.get('width') or a_w
            h = rep.get('height') or a_h
            mime = rep.get('mimeType') or a_mime
            # Skip audio/text/image reps. Trickmode/thumbnail sets carry width/height
            # too, so an explicit non-video marker must win over the dimension check;
            # the (w and h) fallback only rescues video reps whose set omitted a type.
            _mime_l, _ct_l = (mime or '').lower(), (a_ct or '').lower()
            _is_nonvideo = any(t in _mime_l or t in _ct_l for t in ('image', 'audio', 'text'))
            _is_video = 'video' in _mime_l or 'video' in _ct_l
            if _is_nonvideo or (not _is_video and not (w and h)):
                continue  # audio/text/thumbnail rep
            v: dict = {}
            bw = rep.get('bandwidth')
            if bw and bw.isdigit():
                v['bandwidth'] = int(bw)
            if w and h:
                v['resolution'] = f'{w}x{h}'
            codecs = rep.get('codecs') or a_codecs
            if codecs:
                v['_raw_codecs'] = codecs
                v['codecs'] = _friendly_codecs(codecs)
            fps = _fps(rep.get('frameRate') or a_fr)
            if fps:
                v['fps'] = fps
            variants.append(v)

    return _build_stream_info(variants)


def _build_stream_info(variants: list[dict]) -> dict | None:
    """Shared finalizer: take per-variant dicts (resolution/bandwidth/codecs/fps) and
    derive the max-resolution / codec / quality-tier summary. Used by both the HLS and
    DASH parsers so their output is identical."""
    if not variants:
        return None

    variants.sort(key=lambda v: v.get('bandwidth', 0), reverse=True)

    max_w = max_h = 0
    max_resolution = None
    for v in variants:
        res = v.get('resolution', '')
        if res and 'x' in res:
            try:
                w, h = (int(x) for x in res.split('x', 1))
                if w * h > max_w * max_h:
                    max_w, max_h = w, h
                    max_resolution = res
            except ValueError:
                pass

    video_codec = 'unknown'
    for v in variants:
        for part in v.get('_raw_codecs', '').split(','):
            prefix = part.strip().split('.')[0].lower()
            if prefix in _VIDEO_CODEC_MAP:
                video_codec = _VIDEO_CODEC_MAP[prefix]
                break
        if video_codec != 'unknown':
            break

    # No variant advertised RESOLUTION (e.g. Pluto) — estimate height from the
    # top BANDWIDTH rung so the channel still gets a quality tier. Each variant
    # is tagged so the UI can mark these as approximate.
    resolution_estimated = False
    if not max_h:
        for v in variants:
            est_h = estimate_height_from_bandwidth(v.get('bandwidth'), video_codec)
            if est_h:
                v['resolution'] = _NOMINAL_DIMS[est_h]
                v['resolution_est'] = True
        top_h = estimate_height_from_bandwidth(
            variants[0].get('bandwidth') if variants else None, video_codec)
        if top_h:
            max_h = top_h
            max_w = int(_NOMINAL_DIMS[top_h].split('x', 1)[0])
            max_resolution = _NOMINAL_DIMS[top_h]
            resolution_estimated = True

    clean_variants = [{k: val for k, val in v.items() if k != '_raw_codecs'} for v in variants]

    return {
        'max_resolution': max_resolution,
        'max_width':      max_w or None,
        'max_height':     max_h or None,
        'video_codec':    video_codec,
        'has_4k':         max_h >= 2160 if max_h else False,
        'has_hd':         max_h >= 720  if max_h else False,
        'resolution_estimated': resolution_estimated,
        'variants':       clean_variants,
    }
_ATTR_RE = re.compile(r'([A-Z0-9-]+)=(".*?"|[^,]+)', re.IGNORECASE)
_DRM_METHODS = {'SAMPLE-AES', 'SAMPLE-AES-CTR', 'SAMPLE-AES-CENC'}
# Match both #EXT-X-KEY and #EXT-X-SESSION-KEY (used in CMAF multi-DRM manifests)
_EXT_X_KEY_ANY_RE = re.compile(r'^#EXT-X-(?:SESSION-)?KEY:(.+)$', re.IGNORECASE)


def _parse_attrs(attr_text: str) -> dict[str, str]:
    attrs: dict[str, str] = {}
    for key, raw_value in _ATTR_RE.findall(attr_text):
        value = raw_value.strip()
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        attrs[key.upper()] = value
    return attrs


def inspect_hls_drm(manifest_text: str) -> dict | None:
    """
    Inspect HLS manifest text for client-breaking DRM/encryption.

    Scans all #EXT-X-KEY and #EXT-X-SESSION-KEY lines (both forms appear in
    CMAF multi-DRM manifests).  If Widevine is present in any entry, returns
    None — the stream is Widevine-capable and should not be disabled.  FairPlay
    appearing first in Irdeto/CMAF manifests alongside Widevine is a red herring;
    only FairPlay-only or PlayReady-only streams are truly unplayable on most
    clients.

    Plain AES-128 (METHOD=AES-128, no KEYFORMAT) is not flagged.
    """
    found: list[dict] = []
    has_widevine = False

    for raw_line in manifest_text.splitlines():
        line = raw_line.strip()
        match = _EXT_X_KEY_ANY_RE.match(line)
        if not match:
            continue
        attrs = _parse_attrs(match.group(1))
        method = (attrs.get('METHOD') or '').strip().upper()
        uri = (attrs.get('URI') or '').strip()
        keyformat = (attrs.get('KEYFORMAT') or '').strip()
        if not method or method == 'NONE' or not uri:
            continue

        drm_type = None
        keyformat_lower = keyformat.lower()
        if keyformat and keyformat_lower != 'identity':
            if 'widevine' in keyformat_lower or WIDEVINE_UUID in keyformat_lower:
                drm_type = 'Widevine'
                has_widevine = True
            elif 'fairplay' in keyformat_lower or 'apple' in keyformat_lower or 'com.apple.streamingkeydelivery' in keyformat_lower:
                drm_type = 'FairPlay'
            elif 'playready' in keyformat_lower or 'microsoft' in keyformat_lower or PLAYREADY_UUID in keyformat_lower:
                drm_type = 'PlayReady'
            else:
                drm_type = f'Unknown (KEYFORMAT={keyformat})'

        if drm_type or method in _DRM_METHODS:
            found.append({
                'method': method,
                'uri': uri,
                'keyformat': keyformat or None,
                'drm_type': drm_type or f'Encrypted ({method})',
            })

    if not found:
        return None

    # CMAF multi-DRM: Widevine present means the stream is Widevine-capable.
    # Don't block — FairPlay keys co-existing with Widevine in the same manifest
    # are for Apple clients; the stream is not FairPlay-only.
    if has_widevine:
        return None

    return found[0]
