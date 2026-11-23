"""Health check for a Channels DVR capture stream (the Single-port HDMI Capture path).

Channels DVR does the capture and transcode itself and FastChannels cannot see its
ffmpeg log, so this measures what we *can* see: a short sample of the exported
``stream.mpg``.  It reports the stream format, whether media arrives at real-time
speed, the delivered frame rate, timestamp gaps and (on a longer sample) A/V drift.
"""
import json
import os
import subprocess
import tempfile
import time

import requests

QUICK_SECONDS = 15
DRIFT_SECONDS = 60

# AAC frames are ~21 ms, so offsets/drift below that are measurement noise.
_DRIFT_WARN_MS_PER_MIN = 50
_DRIFT_BAD_MS_PER_MIN = 150
_REALTIME_WARN = 0.97
_REALTIME_BAD = 0.9
_FPS_NOTE_RATIO = 0.95   # healthy cards already deliver ~8% under their label


def _probe(path: str, *args) -> dict:
    out = subprocess.run(['ffprobe', '-v', 'error', '-of', 'json', *args, path],
                         capture_output=True, text=True, timeout=90)
    try:
        return json.loads(out.stdout or '{}')
    except ValueError:
        return {}


def _rate(text: str) -> float:
    try:
        num, den = str(text).split('/')
        return float(num) / float(den) if float(den) else 0.0
    except (ValueError, ZeroDivisionError):
        return 0.0


def _packets(path: str, selector: str) -> list[tuple[int, float]]:
    """(byte position, pts seconds) per packet, in file order."""
    packets = _probe(path, '-show_entries', 'packet=pos,pts_time',
                     '-select_streams', selector).get('packets', [])
    return [(int(p['pos']), float(p['pts_time'])) for p in packets
            if p.get('pts_time') and str(p.get('pos', '')).isdigit()]


def _mean_offset(apk, vpk, lo: float, hi: float) -> float | None:
    """Mean (audio pts - video pts) for audio packets in [lo, hi) seconds, each paired
    with the video packet nearest in the file.  Packets near each other in the file
    were muxed together, so this averages out per-packet interleave jitter."""
    import bisect
    vpos = [p for p, _ in vpk]
    diffs = []
    for pos, apts in apk:
        if not lo <= apts < hi:
            continue
        i = bisect.bisect_left(vpos, pos)
        near = min((j for j in (i - 1, i) if 0 <= j < len(vpk)), key=lambda j: abs(vpos[j] - pos), default=None)
        if near is not None:
            diffs.append(apts - vpk[near][1])
    return sum(diffs) / len(diffs) if diffs else None


def sample_stream(url: str, seconds: int) -> dict:
    """Read ``seconds`` of the stream and return raw measurements.

    Raises ``requests.RequestException`` if the stream can't be opened.
    """
    tmp = tempfile.NamedTemporaryFile(suffix='.ts', delete=False)
    tmp.close()
    try:
        started = time.monotonic()
        first = None
        received = 0
        with requests.get(url, stream=True, timeout=(10, 20)) as resp:
            resp.raise_for_status()
            with open(tmp.name, 'wb') as fh:
                for chunk in resp.iter_content(65536):
                    if not chunk:
                        continue
                    if first is None:
                        first = time.monotonic()
                    fh.write(chunk)
                    received += len(chunk)
                    if time.monotonic() - first >= seconds:
                        break
        if first is None or received == 0:
            return {'error': 'The capture stream opened but sent no data.'}
        wall = time.monotonic() - first

        info = _probe(tmp.name, '-show_streams')
        streams = info.get('streams', [])
        video = next((s for s in streams if s.get('codec_type') == 'video'), {})
        audio = next((s for s in streams if s.get('codec_type') == 'audio'), {})
        vpk = _packets(tmp.name, 'v')
        apk = _packets(tmp.name, 'a')
        vpts = sorted(t for _, t in vpk)
        media = (vpts[-1] - vpts[0]) if len(vpts) > 1 else 0.0

        fps = _rate(video.get('avg_frame_rate', '0/1'))
        interval = 1 / fps if fps else 0.0
        gaps = [b - a for a, b in zip(vpts, vpts[1:])]
        long_gaps = [g for g in gaps if interval and g > interval * 1.5]
        dropped = sum(max(0, round(g / interval) - 1) for g in long_gaps) if interval else 0

        drift = None
        start = end = None
        if media >= 30 and apk and vpk:
            t0, t1 = vpts[0], vpts[-1]
            start = _mean_offset(apk, vpk, t0, t0 + 5)
            end = _mean_offset(apk, vpk, t1 - 5, t1)
        if start is not None and end is not None:
            drift = {
                'start_offset_ms': round(start * 1000),
                'end_offset_ms': round(end * 1000),
                'drift_ms': round((end - start) * 1000),
                'ms_per_min': round((end - start) * 1000 * 60 / media, 1),
            }

        return {
            'seconds': round(wall, 1),
            'startup_s': round(first - started, 2),
            'realtime_x': round(media / wall, 3) if wall else None,
            'kbps': round(received * 8 / wall / 1000) if wall else None,
            'video': {
                'codec': video.get('codec_name'),
                'width': video.get('width'), 'height': video.get('height'),
                'label_fps': round(fps, 2),
                'delivered_fps': round((len(vpts) - 1) / media, 1) if media else None,
                'dropped': dropped, 'gaps': len(long_gaps),
            },
            'audio': {
                'codec': audio.get('codec_name'), 'channels': audio.get('channels'),
                'rate': audio.get('sample_rate'),
            },
            'av_start_offset_ms': round(((min(t for _, t in apk) if apk else 0) - (vpts[0] if vpts else 0)) * 1000),
            'drift': drift,
        }
    finally:
        try:
            os.unlink(tmp.name)
        except OSError:
            pass


def verdicts(result: dict) -> list[dict]:
    """Plain-language findings: [{level: ok|note|warn|bad, text}]."""
    out = []
    if result.get('error'):
        return [{'level': 'bad', 'text': result['error']}]

    rt = result.get('realtime_x')
    if rt is not None:
        if rt < _REALTIME_BAD:
            out.append({'level': 'bad', 'text': (
                f'Media is arriving at {rt:.2f}x real time, so the capture/transcode is falling behind. '
                'This is what causes dropped frames and buffer warnings. Try a lower frame rate or '
                'resolution, or check CPU load on the Channels DVR computer.')})
        elif rt < _REALTIME_WARN:
            out.append({'level': 'warn', 'text': (
                f'Media is arriving at {rt:.2f}x real time, which is slightly slow. '
                'Watch CPU load on the Channels DVR computer.')})
        else:
            out.append({'level': 'ok', 'text': 'Keeping up with real time.'})

    v = result.get('video') or {}
    label, got = v.get('label_fps'), v.get('delivered_fps')
    if label and got and got < label * _FPS_NOTE_RATIO:
        out.append({'level': 'note', 'text': (
            f'The stream is labeled {label:g} fps but about {got:g} fps arrive. Many capture cards '
            'run a little under their label. If motion looks uneven, try a 30 fps capture URL.')})
    if v.get('dropped'):
        out.append({'level': 'warn' if v['dropped'] > 5 else 'note',
                    'text': f"{v['dropped']} frame(s) missing in this sample."})

    a = result.get('audio') or {}
    if not a.get('codec'):
        out.append({'level': 'bad', 'text': 'No audio track in the stream. Check the audio device in the capture URL.'})

    d = result.get('drift')
    if d:
        rate = abs(d['ms_per_min'])
        if rate >= _DRIFT_BAD_MS_PER_MIN:
            out.append({'level': 'bad', 'text': (
                f"Audio and video drift apart by about {d['ms_per_min']:+g} ms per minute "
                'in the capture stream itself.')})
        elif rate >= _DRIFT_WARN_MS_PER_MIN:
            out.append({'level': 'warn', 'text': f"Possible A/V drift: {d['ms_per_min']:+g} ms per minute."})
        else:
            out.append({'level': 'ok', 'text': (
                f"No A/V drift over {result.get('seconds', 0):.0f} s ({d['drift_ms']:+d} ms, "
                'within measurement noise).')})
    return out
