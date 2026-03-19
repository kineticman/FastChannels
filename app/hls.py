import re


_EXT_X_KEY_RE = re.compile(r'^#EXT-X-KEY:(.+)$', re.IGNORECASE)
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
        if keyformat:
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
