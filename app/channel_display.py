import re


_COX_CALL_SIGN_SUFFIX_RE = re.compile(r'\s+\(([A-Z0-9]{2,12})\)\s*$')


def display_channel_name(channel) -> str:
    """Return the UI-facing channel name without changing stored metadata."""
    name = (getattr(channel, 'name', None) or '').strip()
    source = getattr(channel, 'source', None)
    source_name = (getattr(source, 'name', None) or '').strip().lower()
    if source_name != 'cox':
        return name
    return _COX_CALL_SIGN_SUFFIX_RE.sub('', name).strip() or name
