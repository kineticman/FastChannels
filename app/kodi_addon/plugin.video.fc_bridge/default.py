"""FastChannels Bridge resolver addon.

Takes a manifest URL and a Widevine license URL as plugin:// query params and hands
them to inputstream.adaptive, replicating the #KODIPROP block FastChannels used to have
to bake into a temporary hosted M3U file for every channel trigger.

Example invocation (what FastChannels' backend calls via JSON-RPC Player.Open):

    plugin://plugin.video.fc_bridge/?url=<urlencoded manifest url>&license=<urlencoded license url>

Optional params:
    license_type    - defaults to com.widevine.alpha
    name            - display name shown in Kodi's UI while playing
    single_session  - "1" to use the new Kodi v22+ inputstream.adaptive.drm JSON
                      property with force_single_session=true instead of the legacy
                      license_type/license_key properties. Opt-in and experimental —
                      being tried against sources (Cox) whose backend only grants one
                      license per playback attempt, where inputstream.adaptive's
                      default one-CDM-session-per-track behavior fails. Every other
                      source keeps using the legacy properties, proven working across
                      the Kodi 21->22 upgrade (dev/kodi/README.md).
    live_delay      - seconds to hold playback behind the absolute live edge, passed
                      straight through to inputstream.adaptive.live_delay (supported
                      on the installed Omega/21.x branch — confirmed via
                      github.com/xbmc/inputstream.adaptive/wiki/Integration). Default
                      20s. Intended to mitigate problems caused by requesting segments/
                      Periods that don't exist yet right at the live edge — upstream
                      docs recommend 2-4x segment duration, floor 16s. Experimental:
                      added 2026-08-24 to see whether it reduces the freeze/stall rate
                      around ad-break Period transitions on kodi-bridge sources (see
                      xbmc/inputstream.adaptive#1507 — ad-break/X-discontinuity
                      handling is an acknowledged, still-open upstream gap, not
                      something specific to this bridge).
"""
import json
import sys
from urllib.parse import parse_qsl, urlparse

import xbmcgui
import xbmcplugin

DEFAULT_LICENSE_TYPE = 'com.widevine.alpha'
DEFAULT_LIVE_DELAY_S = '20'


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
    single_session = params.get('single_session') == '1'
    live_delay = params.get('live_delay', DEFAULT_LIVE_DELAY_S)

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
        if live_delay:
            item.setProperty('inputstream.adaptive.live_delay', str(live_delay))
        if single_session:
            item.setProperty(
                'inputstream.adaptive.manifest_type',
                'hls' if manifest_url.endswith('.m3u8') else 'mpd',
            )
            item.setProperty('inputstream.adaptive.drm', json.dumps({
                license_type: {
                    'license': {
                        'server_url': license_url,
                        'req_headers': 'Content-Type=application%2Foctet-stream',
                    },
                    'force_single_session': True,
                },
            }))
        else:
            item.setProperty('inputstream.adaptive.license_type', license_type)
            item.setProperty(
                'inputstream.adaptive.license_key',
                f'{license_url}|Content-Type=application%2Foctet-stream|R{{SSM}}|',
            )

    xbmcplugin.setResolvedUrl(handle, True, listitem=item)


if __name__ == '__main__':
    main()
