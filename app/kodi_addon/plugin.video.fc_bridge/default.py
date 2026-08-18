"""FastChannels Bridge resolver addon.

Takes a manifest URL and a Widevine license URL as plugin:// query params and hands
them to inputstream.adaptive, replicating the #KODIPROP block FastChannels used to have
to bake into a temporary hosted M3U file for every channel trigger.

Example invocation (what FastChannels' backend calls via JSON-RPC Player.Open):

    plugin://plugin.video.fc_bridge/?url=<urlencoded manifest url>&license=<urlencoded license url>

Optional params:
    license_type  - defaults to com.widevine.alpha
    name          - display name shown in Kodi's UI while playing
"""
import sys
from urllib.parse import parse_qsl, urlparse

import xbmcgui
import xbmcplugin

DEFAULT_LICENSE_TYPE = 'com.widevine.alpha'


def _params() -> dict:
    query = urlparse(sys.argv[0] + sys.argv[2]).query
    return dict(parse_qsl(query))


def main() -> None:
    handle = int(sys.argv[1])
    params = _params()

    manifest_url = params.get('url')
    license_url = params.get('license')
    license_type = params.get('license_type', DEFAULT_LICENSE_TYPE)
    name = params.get('name', 'FastChannels')

    if not manifest_url:
        xbmcplugin.setResolvedUrl(handle, False, xbmcgui.ListItem())
        return

    item = xbmcgui.ListItem(label=name, path=manifest_url)
    item.setProperty('IsPlayable', 'true')
    item.setContentLookup(False)
    if manifest_url.endswith('.m3u8'):
        item.setMimeType('application/vnd.apple.mpegurl')
    elif manifest_url.endswith('.mpd'):
        item.setMimeType('application/dash+xml')

    if license_url:
        item.setProperty('inputstream', 'inputstream.adaptive')
        item.setProperty('inputstream.adaptive.license_type', license_type)
        item.setProperty(
            'inputstream.adaptive.license_key',
            f'{license_url}|Content-Type=application%2Foctet-stream|R{{SSM}}|',
        )

    xbmcplugin.setResolvedUrl(handle, True, listitem=item)


if __name__ == '__main__':
    main()
