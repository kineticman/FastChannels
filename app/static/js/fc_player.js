/* fc_player.js — shared Shaka HLS/DASH playback engine for FastChannels.
 *
 * Owns only the stream-loading engine: native-vs-Shaka routing, the Safari/iOS
 * native-HLS fallback, DRM config, and the shaka.load() call. Page-specific UX
 * (layout, status DOM, button rows, stats overlays, show/teardown) stays in each
 * template — the caller wires those in through the callbacks below.
 *
 * Usage:
 *   const player = FCPlayer.load({
 *     video, url, mode, type, license,
 *     onStatus: (text) => { ... },          // info: loading / loaded / native-mode notes
 *     onError:  (info) => { ... },          // info = { phase, err, message }
 *   });
 *   // `player` is the shaka.Player instance, or null for native playback.
 *   // The caller owns its lifetime: keep the reference for stats and destroy it
 *   // on teardown.
 *
 * onError phases: 'browser-unsupported', 'drm-insecure', 'event' (runtime error
 * during playback), 'load' (load() promise rejected). `message` is a verbose,
 * human-readable string; pages that want terse text can switch on phase/err.code.
 */
(function () {
  'use strict';

  function isSafariOrIos() {
    const ua = navigator.userAgent;
    return (/Safari/.test(ua) && !/Chrome/.test(ua)) || /iPad|iPhone|iPod/.test(ua);
  }

  function formatErrorData(data) {
    if (!Array.isArray(data) || !data.length) return '';
    try {
      return JSON.stringify(data, (key, value) => {
        if (typeof value === 'string' && /^https?:\/\//i.test(value)) {
          try {
            const parsed = new URL(value);
            return `${parsed.origin}${parsed.pathname}`;
          } catch (_) {
            return value;
          }
        }
        if (value instanceof Error) return { name: value.name, message: value.message };
        return value;
      }).slice(0, 500);
    } catch (_) {
      return String(data).slice(0, 500);
    }
  }

  function formatError(err) {
    if (!err) return 'Shaka playback failed (no error details).';

    const code = err.code || 'unknown';
    const codeName = Object.entries(window.shaka?.util?.Error?.Code || {})
      .find(([, value]) => value === err.code)?.[0];
    const categoryName = Object.entries(window.shaka?.util?.Error?.Category || {})
      .find(([, value]) => value === err.category)?.[0];
    const context = [];

    if (err.code === 4042) {
      context.push(`Web Crypto unavailable (secure context: ${window.isSecureContext}; crypto.subtle: ${Boolean(window.crypto?.subtle)})`);
    } else if (err.code === 4040) {
      context.push('encrypted MPEG-TS cannot be played through MediaSource in this browser');
    } else if (err.code === 6007) {
      context.push('DRM license request failed; check the license endpoint, authorization, and network response');
    } else if (err.code === 6008) {
      context.push('the browser CDM rejected the DRM license response');
    } else if (err.code === 6014) {
      context.push('the DRM license has expired');
    } else if (err.code === 6020) {
      context.push(`browser DRM APIs unavailable (secure context: ${window.isSecureContext})`);
    } else if (err.code === 1001 || err.code === 1002) {
      context.push('manifest or segment request failed; check HTTP status, CORS, and network filtering');
    }

    const details = formatErrorData(err.data);
    const identity = [categoryName, codeName].filter(Boolean).join('.');
    return [
      `Shaka error ${code}${identity ? ` (${identity})` : ''}: ${err.message || 'playback failed'}.`,
      ...context,
      details ? `Details: ${details}` : '',
    ].filter(Boolean).join(' ');
  }

  function seekNearLiveEdge(player, video, opts) {
    try {
      if (!player || !video || !player.isLive || !player.isLive()) return;
      const range = player.seekRange && player.seekRange();
      if (!range || !Number.isFinite(range.end) || range.end <= 0) return;
      const liveDelay = Number((opts && opts.liveDelay) || 3);
      const minSeek = Number((opts && opts.minSeek) || 5);
      const target = Math.max(range.start || 0, range.end - liveDelay);
      const from = video.currentTime || 0;
      // A live seek range that starts well past 0 (DASH periods with a large
      // presentationTimeOffset put the timeline at millions of seconds) means a
      // currentTime of 0 is just "the media element hasn't reported a position
      // yet", NOT a real playhead 33M seconds behind live. Seeking on that bogus
      // delta forces a redundant jump to where Shaka already positioned us, and
      // that jump flushes MSE + the CDM mid-startup: both decoders report
      // "no key for key ID ...", and the first audio packet decoded after the key
      // recovers fails outright (PIPELINE_ERROR_DECODE -> Shaka 3016, then 3015
      // appendBuffer errors once the media element is dead). Shaka already starts
      // live content at the live edge on its own, so skip it.
      if (from < (range.start || 0)) return;
      const delta = target - from;
      if (Number.isFinite(target) && Math.abs(delta) > minSeek) {
        video.currentTime = target;
        if (opts && typeof opts.onSeek === 'function') {
          opts.onSeek({
            from: Number(from.toFixed(3)),
            to: Number(target.toFixed(3)),
            delta: Number(delta.toFixed(3)),
            range_start: Number((range.start || 0).toFixed(3)),
            range_end: Number(range.end.toFixed(3)),
          });
        }
      }
    } catch (_) {}
  }

  /**
   * Load `url` into the `video` element. Returns the shaka.Player instance, or
   * null when playback is handled natively (direct video / native HLS) or cannot
   * proceed (unsupported browser / insecure DRM context).
   */
  function load(opts) {
    const video = opts.video;
    const url = opts.url;
    if (!video || !url) return null;

    const mode = String(opts.mode || '').toLowerCase();
    const type = String(opts.type || '').toLowerCase();
    const license = String(opts.license || '');
    const maxHeight = Number(opts.maxHeight || 0);
    const liveEdgeSync = Boolean(opts.liveEdgeSync);
    const onStatus = opts.onStatus || function () {};
    const onError = opts.onError || function (info) { onStatus(info.message); };

    const isDirect = ['mp4', 'webm', 'mov', 'mkv', 'direct'].includes(type);
    const safari = isSafariOrIos();

    if (isDirect || (mode === 'native' && safari)) {
      video.src = url;
      video.load();
      onStatus('Using native video playback for this stream type.');
      video.play().catch(() => {});
      return null;
    }

    // Safari/iOS: use native HLS to avoid Shaka errors 4040/4042 on AES-128
    // encrypted transport streams that MediaSource can't decrypt. Skip for DASH
    // or DRM streams, which Shaka must handle. Do not use canPlayType() alone:
    // some Chrome environments report HLS support but fail on live MPEG-TS.
    const isDash = mode === 'dash' || type === 'dash';
    const nativeHls = !isDash && !license && safari;
    if (nativeHls) {
      video.src = url;
      video.load();
      onStatus('Using native HLS playback.');
      video.play().catch(() => {});
      return null;
    }

    if (!window.shaka || !shaka.Player.isBrowserSupported()) {
      onError({ phase: 'browser-unsupported', err: null, message: 'Playback is not supported in this browser.' });
      return null;
    }

    if (license && !window.isSecureContext) {
      onError({ phase: 'drm-insecure', err: null, message: 'DRM streams require a secure context (HTTPS or a browser-trusted origin).' });
      return null;
    }

    shaka.polyfill.installAll();
    onStatus('Loading stream…');
    const player = new shaka.Player();
    player.attach(video).then(() => {
      // Prefer plain AAC when a manifest offers several audio codecs. Chrome
      // refuses Widevine outright for ec-3 (Dolby Digital+) — requesting it
      // fails the whole load with DRM.REQUESTED_KEY_SYSTEM_CONFIG_UNAVAILABLE
      // (6001), verified on Chrome/Windows against NBC's dual-codec manifest.
      const shakaConfig = { preferredAudioCodecs: ['mp4a.40.2', 'mp4a.40.5', 'aac'] };
      if (license) {
        shakaConfig.drm = {
          servers: {
            'com.widevine.alpha':      license,
            'com.microsoft.playready': license,
          },
        };
        if (liveEdgeSync) {
          shakaConfig.streaming = {
            bufferingGoal: 20,
            rebufferingGoal: 4,
            bufferBehind: 20,
            liveSync: {
              enabled: true,
              targetLatency: 3,
              targetLatencyTolerance: 2,
              maxPlaybackRate: 1.08,
              minPlaybackRate: 0.95,
            },
          };
        }
        if (maxHeight > 0) {
          shakaConfig.restrictions = { maxHeight };
        }
      }
      player.configure(shakaConfig);
      if (license) {
        player.getNetworkingEngine().registerRequestFilter((reqType, request) => {
          if (reqType === 2 /* LICENSE */) request.uris = [license];
        });
      }
      player.addEventListener('error', (event) => {
        const err = event.detail;
        onError({ phase: 'event', err, message: formatError(err) });
        console.error('[Shaka] error', err.code, err);
      });
      if (liveEdgeSync) {
        let lastLiveSeek = 0;
        const chaseLive = (reason) => {
          const now = Date.now();
          if (now - lastLiveSeek < 2500) return;
          lastLiveSeek = now;
          seekNearLiveEdge(player, video, {
            liveDelay: 3,
            minSeek: 8,
            onSeek: (info) => {
              if (typeof opts.onLiveEdgeSeek === 'function') {
                opts.onLiveEdgeSeek(Object.assign({ reason }, info));
              }
            },
          });
        };
        player.addEventListener('buffering', (event) => {
          if (!event.buffering) window.setTimeout(() => chaseLive('buffering-ended'), 250);
        });
        video.addEventListener('playing', () => window.setTimeout(() => chaseLive('playing'), 250));
      }
      const mimeType = isDash ? 'application/dash+xml' : 'application/x-mpegURL';
      return player.load(url, null, mimeType);
    }).then(() => {
      onStatus('Stream loaded.');
      seekNearLiveEdge(player, video, {
        liveDelay: 3,
        minSeek: liveEdgeSync ? 3 : 5,
        onSeek: (info) => {
          if (typeof opts.onLiveEdgeSeek === 'function') {
            opts.onLiveEdgeSeek(Object.assign({ reason: 'load' }, info));
          }
        },
      });
      // Do NOT unmute here: Chrome's autoplay policy rejects play() on an unmuted
      // element without a user gesture, and that rejection is swallowed below —
      // leaving the stream loaded, fully buffered, and silently paused on a single
      // frame with no error anywhere. Stay muted so autoplay is actually allowed;
      // the page's click/keypress handler unmutes and re-plays on a real gesture.
      video.play().catch(() => {});
    }).catch((err) => {
      onError({ phase: 'load', err, message: formatError(err) });
      console.error('[Shaka] load failed', err);
    });
    return player;
  }

  window.FCPlayer = { load, formatError, isSafariOrIos };
})();
