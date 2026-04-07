import re


_EXT_X_KEY_RE = re.compile(r'^#EXT-X-KEY:(.+)$', re.IGNORECASE)

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

    clean_variants = [{k: val for k, val in v.items() if k != '_raw_codecs'} for v in variants]

    return {
        'max_resolution': max_resolution,
        'max_width':      max_w or None,
        'max_height':     max_h or None,
        'video_codec':    video_codec,
        'has_4k':         max_h >= 2160 if max_h else False,
        'has_hd':         max_h >= 720  if max_h else False,
        'variants':       clean_variants,
    }
_ATTR_RE = re.compile(r'([A-Z0-9-]+)=(".*?"|[^,]+)', re.IGNORECASE)
_DRM_METHODS = {'SAMPLE-AES', 'SAMPLE-AES-CTR', 'SAMPLE-AES-CENC'}


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

    Notes:
    - Do not flag plain AES-128 by itself. Generic HLS clients can often play it.
    - Flag SAMPLE-AES family methods.
    - Flag explicit KEYFORMAT markers for known DRM systems even if the method is
      not in the SAMPLE-AES family.
    """
    for raw_line in manifest_text.splitlines():
        line = raw_line.strip()
        match = _EXT_X_KEY_RE.match(line)
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
            if 'widevine' in keyformat_lower or 'edef8ba9-79d6-4ace-a3c8-27dcd51d21ed' in keyformat_lower:
                drm_type = 'Widevine'
            elif 'fairplay' in keyformat_lower or 'apple' in keyformat_lower or 'com.apple.streamingkeydelivery' in keyformat_lower:
                drm_type = 'FairPlay'
            elif 'playready' in keyformat_lower or 'microsoft' in keyformat_lower or '9a04f079-9840-4286-ab92-e65be0885f95' in keyformat_lower:
                drm_type = 'PlayReady'
            else:
                drm_type = f'Unknown (KEYFORMAT={keyformat})'

        if drm_type or method in _DRM_METHODS:
            return {
                'method': method,
                'uri': uri,
                'keyformat': keyformat or None,
                'drm_type': drm_type or f'Encrypted ({method})',
            }
    return None
