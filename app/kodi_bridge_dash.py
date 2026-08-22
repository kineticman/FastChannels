"""DASH manifest rewriting for the Kodi/Fire TV HDMI-encoder bridge.

Pure text-in/text-out manifest transforms called from each source's `/dash.mpd`
proxy route (app/routes/play.py) when `kodi_bridge=1`. Split out from play.py
(which was growing unwieldy) since these are a self-contained concern: none of
them touch Flask, the DB, or the device itself — see app/kodi_bridge.py for the
JSON-RPC/adb side of the bridge (trigger_channel, the InstanceGuard watchdog,
etc.), which these functions exist to keep out of trouble.

All of this exists because of one MediaTek Fire TV Stick hardware limitation:
its secure decoder only permits one MediaCodec instance at a time
(CDVDVideoCodecAndroidMediaCodec::Open - InstanceGuard locked). inputstream.adaptive
disposes and reinitializes its decrypter on every DASH Period boundary
unconditionally (confirmed via its own source/logs — "New period, dispose sample
decrypter and reinitialize" is standard, not an error path), but only tears down
the underlying secure MediaCodec instance when the codec's negotiated format
actually changes. A same-format Period transition reinitializes the decrypter and
moves on cleanly (confirmed live 2026-08-22: 10 consecutive same-composition
transitions, zero reopens); a format-changing one races the still-releasing prior
instance and freezes playback with no visible error and no signal in
Player.GetProperties (see kodi_bridge.py's trigger_channel() docstring and
project memory for the full investigation). Every function below exists to keep
what Kodi sees composition-stable across Period boundaries, so that mandatory
per-Period reinit never needs to touch the actual MediaCodec instance.
"""
import logging
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)


def _mpd_strip_non_av_tracks(mpd: str, channel_id: str) -> str:
    """Strip every non-video/audio AdaptationSet (timed-metadata, subtitle,
    thumbnail tracks, etc.) from a kodi-bridge manifest.

    Confirmed live 2026-08-22 (Amazon Prime Free, NBC Sports NOW): a live manifest's
    Period boundary where an auxiliary track (an 'application/mp4' timed-metadata
    AdaptationSet, in this case) appears or disappears forces inputstream.adaptive to
    fully reinitialize ALL codecs at that boundary — including video/audio, which were
    otherwise unchanged — and that reinitialization races the still-releasing prior
    MediaCodec instance the same way a real channel change does (InstanceGuard locked,
    see _mpd_keep_highest_bitrate_video below). Confirmed via adb logcat across 10
    consecutive same-composition Period transitions afterward: none triggered a
    reopen, only the composition-changing one did — a stable timeline isn't what
    matters here, a stable set of tracks is. Kodi's HDMI-bridge use case never uses
    these auxiliary tracks (captions come from the video stream's own embedded
    CEA-608/708 data via set_captions_enabled(), not a manifest subtitle track), so
    removing them outright is safe and sidesteps the whole class of composition-change
    reopens regardless of which Periods a source's origin decides to add or drop them
    on. Deliberately narrower than merging Periods (which would need the segment
    timeline/numbering to stay continuous across the join, unverified here) — this
    only ever removes non-AV tracks, never touches video/audio.
    """
    try:
        root = ET.fromstring(mpd.encode('utf-8'))
    except ET.ParseError:
        logger.debug('[dash] MPD parse failed for %s; returning original manifest', channel_id[:40])
        return mpd

    namespace = ''
    if root.tag.startswith('{'):
        namespace = root.tag[1:].split('}', 1)[0]
        ET.register_namespace('', namespace)

    period_tag = f'{{{namespace}}}Period' if namespace else 'Period'
    adaptation_tag = f'{{{namespace}}}AdaptationSet' if namespace else 'AdaptationSet'

    changed = False
    for period in root.iter(period_tag):
        for adaptation_set in list(period.findall(adaptation_tag)):
            content_type = adaptation_set.get('contentType') or adaptation_set.get('mimeType', '')
            if not (content_type.startswith('video') or content_type.startswith('audio')):
                period.remove(adaptation_set)
                changed = True

    if not changed:
        return mpd

    logger.debug('[dash] kodi-bridge: stripped non-AV tracks for %s', channel_id[:40])
    return ET.tostring(root, encoding='unicode', xml_declaration=True)


def _mpd_keep_highest_bitrate(mpd: str, channel_id: str, mime_prefix: str, label: str) -> str:
    """Strip every Representation of the given content type (mime_prefix) except
    the single highest-bandwidth one, collapsing sibling AdaptationSets of that
    type down to one.

    Confirmed live 2026-08-21 (video) on the Kodi HDMI-bridge Fire TV Stick: its
    MediaTek secure decoder only permits one secure MediaCodec instance at a time
    (CDVDVideoCodecAndroidMediaCodec::Open - InstanceGuard locked), so
    inputstream.adaptive's normal mid-stream ABR bump to a better representation
    fails and playback is stuck at whichever representation the codec opened
    first — usually the lowest, since that's what inputstream.adaptive starts
    conservatively. Advertising only the top rendition means the codec opens
    once at full quality and this device never attempts (or needs) a switch.

    Confirmed live 2026-08-22 (audio, PBS Kids): the SAME race also fires across
    a live-manifest Period boundary when a source keeps multiple audio
    AdaptationSets (e.g. a main AAC-LC track plus a separate HE-AAC "descriptive
    audio" group) and one Period's origin drops/reshuffles which Representations
    belong to which group — a composition change _mpd_strip_non_av_tracks doesn't
    catch, since both groups are legitimately audio/*. Collapsing to a single
    audio Representation per Period, picked the same deterministic way (highest
    bandwidth) in every Period, keeps that pick's Representation `id` stable
    across Period boundaries in every case observed so far, avoiding the reopen
    regardless of what alternate audio tracks a source's origin adds or drops.

    Handles both ABR layouts seen across trusted sources: multiple Representations
    inside one AdaptationSet of this type (Roku, Amazon, Sling video), and —
    confirmed on Philo (video) and PBS (audio) — the ladder/alternates split
    across multiple sibling AdaptationSets of the same type instead. Either way,
    exactly one Representation in exactly one AdaptationSet of this type per
    Period survives.
    """
    try:
        root = ET.fromstring(mpd.encode('utf-8'))
    except ET.ParseError:
        logger.debug('[dash] MPD parse failed for %s; returning original manifest', channel_id[:40])
        return mpd

    namespace = ''
    if root.tag.startswith('{'):
        namespace = root.tag[1:].split('}', 1)[0]
        ET.register_namespace('', namespace)

    period_tag = f'{{{namespace}}}Period' if namespace else 'Period'
    adaptation_tag = f'{{{namespace}}}AdaptationSet' if namespace else 'AdaptationSet'
    representation_tag = f'{{{namespace}}}Representation' if namespace else 'Representation'
    supplemental_tag = f'{{{namespace}}}SupplementalProperty' if namespace else 'SupplementalProperty'
    _SWITCHING_SCHEME = 'urn:mpeg:dash:adaptation-set-switching:2016'

    changed = False
    for period in root.iter(period_tag):
        matching_sets = [a for a in period.findall(adaptation_tag) if a.get('mimeType', '').startswith(mime_prefix)]
        best = None  # (bandwidth, adaptation_set, representation)
        for adaptation_set in matching_sets:
            for rep in adaptation_set.findall(representation_tag):
                bandwidth = int(rep.get('bandwidth') or 0)
                if best is None or bandwidth > best[0]:
                    best = (bandwidth, adaptation_set, rep)
        if best is None:
            continue
        _, best_set, best_rep = best
        removed_sibling = False
        for adaptation_set in matching_sets:
            if adaptation_set is best_set:
                for rep in list(adaptation_set.findall(representation_tag)):
                    if rep is not best_rep:
                        adaptation_set.remove(rep)
                        changed = True
            else:
                period.remove(adaptation_set)
                changed = True
                removed_sibling = True
        if removed_sibling:
            # The surviving set's own switching-group cross-reference (confirmed on
            # Philo: <SupplementalProperty schemeIdUri="...adaptation-set-switching:2016"
            # value="<sibling id>"/>) now dangles, pointing at an AdaptationSet id that
            # no longer exists — leaving it in place is what made inputstream.adaptive
            # silently skip requesting the secure decoder for this set (confirmed live
            # 2026-08-21: Philo BBC News opened OMX.MTK.VIDEO.DECODER.AVC, not .secure,
            # and froze — the unfiltered manifest opens .secure correctly).
            for prop in list(best_set.findall(supplemental_tag)):
                if prop.get('schemeIdUri') == _SWITCHING_SCHEME:
                    best_set.remove(prop)
                    changed = True

    if not changed:
        return mpd

    logger.debug('[dash] kodi-bridge: stripped to highest-bitrate %s representation for %s', label, channel_id[:40])
    return ET.tostring(root, encoding='unicode', xml_declaration=True)


def _mpd_keep_highest_bitrate_video(mpd: str, channel_id: str) -> str:
    return _mpd_keep_highest_bitrate(mpd, channel_id, 'video/', 'video')


def _mpd_keep_highest_bitrate_audio(mpd: str, channel_id: str) -> str:
    return _mpd_keep_highest_bitrate(mpd, channel_id, 'audio/', 'audio')

