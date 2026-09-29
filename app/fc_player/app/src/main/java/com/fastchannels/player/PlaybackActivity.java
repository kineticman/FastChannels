package com.fastchannels.player;

import android.app.Activity;
import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.util.Log;
import android.view.WindowManager;

import androidx.media3.common.C;
import androidx.media3.common.Format;
import androidx.media3.common.MediaItem;
import androidx.media3.common.PlaybackException;
import androidx.media3.common.Player;
import androidx.media3.common.Timeline;
import androidx.media3.common.VideoSize;
import androidx.media3.common.util.UnstableApi;
import androidx.media3.datasource.DefaultHttpDataSource;
import androidx.media3.exoplayer.ExoPlayer;
import androidx.media3.exoplayer.source.DefaultMediaSourceFactory;
import androidx.media3.exoplayer.trackselection.DefaultTrackSelector;
import androidx.media3.exoplayer.upstream.DefaultBandwidthMeter;
import androidx.media3.exoplayer.util.EventLogger;
import androidx.media3.session.MediaSession;
import androidx.media3.ui.PlayerView;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileDescriptor;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The entire app. Full-screen playback for FastChannels' remote-play trigger
 * (app/fc_player_bridge.py) — launched via
 * `adb shell am start` with plain Intent extras (no cross-process Serializable involved; see
 * fc_player_bridge.py's docstring for why adb-shell-privileged launch is the mechanism that
 * actually works here). There is no other UI: FastChannels resolves the stream/DRM info
 * server-side and hands it over as Intent extras; this Activity just plays it.
 *
 * Plays the resolved FastChannels URL directly via Media3/ExoPlayer. An earlier design routed
 * playback through the StreamVault-IPTV app instead (a full plugin integration — provider.m3u,
 * playback.prepare, catalog sync into StreamVault's own Live TV UI); that's gone now. Once it
 * became clear this device has no on-device UI need at all (everything is adb-driven from
 * FastChannels), the whole StreamVault dependency — and the fragility that came with it
 * (bind/unbind churn, stale catalog ids, TIF permission walls) — was unnecessary. This Activity
 * needs nothing from StreamVault: MediaItem.DrmConfiguration handles Widevine directly via the
 * device's own CDM, and DefaultMediaSourceFactory's extension-based HLS/DASH auto-detection
 * (FastChannels' resolved URLs always end in .m3u8 or .mpd) needs no manual MediaSource.Factory
 * selection either.
 */
@UnstableApi
public class PlaybackActivity extends Activity {
    private static final String TAG = "FCPlayer.Playback";
    // Deliberately separate from TAG: a real uncaught crash (see the UncaughtExceptionHandler
    // in onCreate) and a Media3-handled onPlayerError are different severities, and
    // fc_player_bridge.check_playback_errors() needs a reliable tag to tell them apart —
    // confirmed live 2026-09-14 that logging both under TAG made a real forced crash
    // (`adb shell am crash`) get classified as a mere "playback error" WARNING instead of a
    // CRASHED ERROR, since the server-side classifier only had the tag to go on.
    private static final String TAG_CRASH = "FCPlayer.Crash";

    static final String EXTRA_STREAM_URL = "stream_url";
    static final String EXTRA_TITLE = "title";
    static final String EXTRA_DRM = "drm";
    static final String EXTRA_LICENSE_URL = "license_url";
    static final String EXTRA_CAPTIONS = "captions";
    static final String EXTRA_COMMAND = "command";
    static final String EXTRA_CHANNEL_KEY = "channel_key";
    static final String COMMAND_WARM_STOP = "warm_stop";

    // Confirmed live 2026-09-14 against a real Vidaa DRM channel: Media3 can hit a fatal
    // internal error (e.g. androidx/media#2440-style SampleQueue/Allocation NPEs) purely from
    // a long-running *live* DASH manifest refreshing under an active renderer read — nothing
    // to do with entitlement or license validity, and calling player.prepare() again recovers
    // cleanly (it rebuilds the renderers/sample queues from the still-valid MediaItem/Timeline).
    // Retrying a bounded number of times before giving up turns this from a full session-ending
    // exit-to-home (indistinguishable from "the app crashed" to a remote user) into something
    // invisible. Bounded + reset-on-recovery so a channel that's genuinely dead (bad entitlement,
    // 404 manifest) still gives up quickly instead of hammering the license server in a loop.
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 2000;
    private static final long RETRY_RESET_AFTER_MS = 60_000;

    // Lever 2 (2026-09-15, Sling clipslist block-boundary investigation): a Sling DRM
    // channel's content resets to a new bounded block every so often (see
    // fc_player_bridge.sling_boundary_status's design note) — the server already knows
    // when and can verify when the new block is actually live, so this app polls that
    // status and swaps its own MediaItem the instant it's confirmed, instead of waiting
    // for playback to fail and either its own bounded retry or the server's adb-trigger
    // watchdog to recover it after the fact. Only engages for the DRM dash.mpd shape
    // (SLING_DASH_URL_RE) — other sources don't have this boundary concept at all.
    private static final Pattern SLING_DASH_URL_RE =
            Pattern.compile("^(https?://[^/]+)/play/sling/([^/]+)/dash\\.mpd");
    private static final long BOUNDARY_POLL_FAR_MS = 30_000;
    private static final long BOUNDARY_POLL_NEAR_MS = 1_500;
    private static final long BOUNDARY_NEAR_WINDOW_S = 15;

    private ExoPlayer player;
    private DefaultTrackSelector trackSelector;
    private MediaSession mediaSession;
    private String activeChannelKey;
    private final Handler retryHandler = new Handler(Looper.getMainLooper());
    private final Runnable resetRetryCount = () -> retryCount = 0;
    private int retryCount = 0;

    // Cached from the most recent playFromIntent() so the Lever 2 swap can rebuild the
    // identical MediaItem without needing a fresh Intent — see reprepare().
    private String activeStreamUrl;
    private boolean activeDrm;
    private String activeLicenseUrl;
    private String boundaryStatusUrl;
    private final ExecutorService boundaryPollExecutor = Executors.newSingleThreadExecutor();
    private final Handler boundaryPollHandler = new Handler(Looper.getMainLooper());
    private Runnable boundaryPollRunnable;

    // Confirmed live 2026-09-24 on a Sling channel: right after an audioTrackUnderrun the
    // player sat in STATE_READY with isPlaying=true for six minutes while its position never
    // moved — no PlaybackException, so onPlayerError never fired and nothing recovered it.
    // Watch the position directly instead. Buffering doesn't count (Media3 owns that, and
    // the Lever 2 swap passes through it); only a player that claims to be playing but isn't.
    private static final long STALL_CHECK_INTERVAL_MS = 5_000;
    private static final long STALL_THRESHOLD_MS = 15_000;
    private static final int MAX_STALL_RECOVERIES = 3;
    private static final long STALL_RECOVERY_RESET_AFTER_MS = 60_000;
    // Sling blocks can contain unannounced ad gaps between periods: segments simply stop
    // (6-58s measured across 6 channels 2026-09-24, most 16-20s) and the manifest only gains
    // the next period partway through. MAX_RETRIES' ~6s budget exits to the launcher at
    // nearly every one. For a live stream, keep rejoining at the live edge until content
    // is back — a fresh MediaItem, since prepare() alone would re-request the same missing
    // segment.
    private static final long LIVE_GAP_RETRY_DELAY_MS = 5_000;
    private static final long LIVE_GAP_RETRY_WINDOW_MS = 120_000;
    private long liveGapStartedAtMs = 0;
    private boolean activeIsLive = false;

    // Read by FastChannels' Bridge devices card over adb (bridge_devices._device_extras)
    // via `dumpsys activity`, which calls dump() below on the main thread: what's on
    // screen and what this app recovered from recently. Not logcat — this device's ring
    // buffer is ~256KB and system chatter rolls it over within a minute — and not a file
    // adb reads directly, since Fire OS denies shell access to Android/data (confirmed
    // 2026-09-24). The file only persists recovery events, so a give-up still shows after
    // the next tune starts a fresh Activity.
    private static final String STATUS_FILE = "status.json";
    private static final String STATUS_DUMP_PREFIX = "FCPLAYER_STATUS ";
    private static final int STATUS_MAX_EVENTS = 20;
    private final ExecutorService statusExecutor = Executors.newSingleThreadExecutor();
    private final ArrayDeque<JSONObject> statusEvents = new ArrayDeque<>();
    private final Handler stallHandler = new Handler(Looper.getMainLooper());
    private final Runnable checkStall = this::checkStall;
    private long lastStallPosMs = -1;
    private long lastAdvanceAtMs;
    private long lastStallRecoveryAtMs;
    private int stallRecoveries = 0;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        // onPlayerError only ever fires for errors Media3's own internal playback thread
        // catches and wraps into a PlaybackException (confirmed live 2026-09-14 — that's
        // ExoPlayerImplInternal's own defensive try/catch around its message loop, not
        // anything in our code). A bug anywhere else — this method, onNewIntent,
        // playFromIntent, a MediaSession callback, PlayerView's surface handling — runs
        // outside that protection and would previously force-close with nothing logged
        // anywhere FastChannels could see (fc_player_bridge.check_playback_errors() only
        // ever tailed the FCPlayer.Playback tag). Log under our own tag FIRST, then chain
        // to the platform's default handler so the crash still proceeds exactly as it
        // otherwise would (dialog/relaunch/process death) — never try to keep running
        // after an uncaught exception on an arbitrary thread, the JVM state past that
        // point isn't trustworthy.
        final Thread.UncaughtExceptionHandler platformHandler = Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler((thread, throwable) -> {
            Log.e(TAG_CRASH, "UNCAUGHT on thread " + thread.getName()
                    + " channel_key=" + activeChannelKey + ": " + throwable, throwable);
            if (platformHandler != null) {
                platformHandler.uncaughtException(thread, throwable);
            }
        });

        getWindow().addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON
                | WindowManager.LayoutParams.FLAG_FULLSCREEN);

        PlayerView playerView = new PlayerView(this);
        // No remote/touch input ever reaches this device (everything is adb-triggered), so the
        // play/pause/seek overlay has nothing to control and no way to be dismissed — just video.
        playerView.setUseController(false);
        setContentView(playerView);

        // Some sources' manifest routes 302 out to their own CDN over https (Roku's
        // osm.sr.roku.com, confirmed live 2026-08-25), while others proxy manifest content
        // directly (200). Media3's DefaultHttpDataSource rejects http->https redirects unless
        // explicitly allowed, failing with InvalidResponseCodeException: Response code: 302
        // instead of following it — Kodi's inputstream.adaptive followed these fine, so this
        // was invisible until the Roku bridge channels were tested against this player.
        //
        // Building our own DataSource.Factory this way has a side effect that isn't obvious:
        // ExoPlayer.Builder's DEFAULT wiring (no custom MediaSourceFactory at all) attaches its
        // BandwidthMeter to the data source it creates internally, so real segment-download
        // measurements feed the adaptive track selector. Replacing that data source factory
        // with our own — needed for the redirect fix above — breaks that connection unless we
        // reattach it ourselves. Confirmed live 2026-08-25: Sling ESPN got stuck at the lowest
        // rendition (512x288, ~320Kbps) for 70+ seconds straight despite a fine Wi-Fi link
        // (585Mbps, RSSI -56) and an unproxied direct-to-CDN segment path — the adaptive
        // selector never had real throughput data to act on, so it just stayed at its
        // conservative initial estimate forever. Explicitly building one BandwidthMeter and
        // wiring it into both the player and the data source factory closes that loop.
        DefaultBandwidthMeter bandwidthMeter = new DefaultBandwidthMeter.Builder(this).build();
        DefaultHttpDataSource.Factory httpDataSourceFactory =
                new DefaultHttpDataSource.Factory()
                        .setAllowCrossProtocolRedirects(true)
                        .setTransferListener(bandwidthMeter);

        // Media3's DefaultTrackSelector leaves text-track selection off by default even
        // when a stream advertises one — an app has to opt in. PlayerView's default UI
        // already includes a SubtitleView overlay, so enabling selection here is the only
        // piece needed. Covers both a real sidecar subtitle rendition (e.g. DirecTV's HLS
        // EXT-X-MEDIA:TYPE=SUBTITLES) and CEA-608/708 captions muxed directly into the
        // video stream via EXT-X-MEDIA:TYPE=CLOSED-CAPTIONS (e.g. Roku) — Media3's HLS
        // playlist parser already exposes the latter as an ordinary selectable text track
        // (application/cea-608) with no extra extractor wiring needed; confirmed live
        // 2026-08-25 rendering real captions on both DirecTV and Roku.
        trackSelector = new DefaultTrackSelector(this);

        player = new ExoPlayer.Builder(this)
                .setTrackSelector(trackSelector)
                .setBandwidthMeter(bandwidthMeter)
                .setMediaSourceFactory(new DefaultMediaSourceFactory(this)
                        .setDataSourceFactory(httpDataSourceFactory))
                .build();
        // Diagnostic only (2026-09-15, Sling clipslist block-boundary investigation):
        // onPlayerError only ever shows the final fatal exception after 3 retries have
        // already failed — this traces every individual manifest/segment load attempt
        // (onLoadError fires per-chunk, before the retry policy decides fatal or not),
        // so a boundary failure shows the actual requested segment URL/number instead of
        // just "Source error". Cheap (log calls only, no network/IO of its own) — left in
        // rather than gated behind a flag since there's no on-device UI to toggle it from.
        player.addAnalyticsListener(new EventLogger(trackSelector, "FCPlayer.EventLogger"));
        // Without this, a failed tune (entitlement rejection, DRM license denial, decode
        // error — ExoPlayer treats all of these as a fatal PlaybackException) left the
        // Activity sitting on a blank surface forever: nothing observed the error, so
        // there was no log line, no retry, and no finish(). Confirmed live via a
        // community bug report 2026-09-12 (both an unentitled channel and an entitled
        // one hung this way). Logging under TAG here is also what
        // fc_player_bridge.check_playback_errors() tails via `adb logcat -s
        // FCPlayer.Playback:E` to surface the failure in FastChannels' own logs.
        //
        // A bounded retry (see MAX_RETRIES) runs first: player.prepare() alone recovers from
        // the transient internal Media3 errors this class of live-DASH-DRM failure produces
        // (confirmed live 2026-09-14, see MAX_RETRIES comment) without needing a fresh
        // MediaItem/license fetch. Only after retries are exhausted does this fall back to the
        // original finish()-and-exit-to-home behavior.
        player.addListener(new Player.Listener() {
            @Override
            public void onPlayerError(PlaybackException error) {
                if (isLiveGapError(error)) {
                    retryHandler.removeCallbacks(resetRetryCount);
                    long now = System.currentTimeMillis();
                    if (liveGapStartedAtMs == 0) liveGapStartedAtMs = now;
                    long elapsedMs = now - liveGapStartedAtMs;
                    if (elapsedMs < LIVE_GAP_RETRY_WINDOW_MS) {
                        Log.w(TAG, "live stream unavailable for " + elapsedMs / 1000 + "s ("
                                + error.getErrorCodeName() + "), rejoining live edge channel_key="
                                + activeChannelKey);
                        retryHandler.postDelayed(PlaybackActivity.this::reprepare, LIVE_GAP_RETRY_DELAY_MS);
                        recordStatusEvent("gap_rejoin");
                    } else {
                        Log.e(TAG, "live stream still unavailable after " + elapsedMs / 1000
                                + "s, giving up channel_key=" + activeChannelKey + ": " + error, error);
                        recordStatusEvent("gave_up");
                        finish();
                    }
                    return;
                }
                Log.e(TAG, "playback error channel_key=" + activeChannelKey
                        + " (retry " + retryCount + "/" + MAX_RETRIES + "): " + error, error);
                retryHandler.removeCallbacks(resetRetryCount);
                if (retryCount < MAX_RETRIES) {
                    retryCount++;
                    recordStatusEvent("error_retry");
                    retryHandler.postDelayed(() -> {
                        Log.i(TAG, "retrying playback channel_key=" + activeChannelKey);
                        player.prepare();
                    }, RETRY_DELAY_MS);
                } else {
                    Log.e(TAG, "giving up after " + MAX_RETRIES + " retries channel_key=" + activeChannelKey);
                    recordStatusEvent("gave_up");
                    finish();
                }
            }

            @Override
            public void onIsPlayingChanged(boolean isPlaying) {
                // A retry that actually holds for a while (as opposed to erroring again
                // immediately) means the stream has genuinely recovered, not just a channel
                // that's permanently broken — reset the budget so a later, unrelated transient
                // error still gets its own full set of retries instead of inheriting an
                // exhausted counter from hours ago.
                if (isPlaying) {
                    liveGapStartedAtMs = 0;
                    retryHandler.postDelayed(resetRetryCount, RETRY_RESET_AFTER_MS);
                } else {
                    retryHandler.removeCallbacks(resetRetryCount);
                }
            }

            @Override
            public void onTimelineChanged(Timeline timeline, int reason) {
                // Remembered because a gap retry swaps in a fresh MediaItem whose timeline
                // isn't known yet when its first error arrives.
                if (player.isCurrentMediaItemLive()) activeIsLive = true;
            }
        });
        playerView.setPlayer(player);
        playerView.setKeepScreenOn(true);

        // No lock-screen/notification controls needed — this device has no on-device UI
        // (see class docstring) — just publishing PlaybackState to dumpsys media_session so
        // an ah4c-driven tuner's stock is_media_playing() check (adb shell dumpsys
        // media_session, looking for a PLAYING PlaybackState — rendered "state=3" on Fire OS,
        // "state=PLAYING(3)" on newer AOSP) can see this player the same way it already sees
        // Hulu/YouTube TV, instead of always reading "not playing".
        // Built once here and left wrapping the same ExoPlayer instance across retunes
        // (see onNewIntent), so it needs no per-retune handling.
        mediaSession = new MediaSession.Builder(this, player).build();

        loadStatusEvents();
        playFromIntent(getIntent());
    }

    /**
     * launchMode="singleTask" (needed so repeated adb am start calls reuse the same task
     * instead of stacking) means a second trigger while this Activity is already resumed
     * arrives here, NOT in onCreate() — the instance and its ExoPlayer stay alive. Confirmed
     * live 2026-08-25: without this override, a trigger for a new channel while one was
     * already playing silently did nothing — the old MediaItem just kept playing, since
     * nothing ever re-read the new Intent's extras.
     */
    @Override
    protected void onNewIntent(Intent intent) {
        super.onNewIntent(intent);
        setIntent(intent);
        playFromIntent(intent);
    }

    private void playFromIntent(Intent intent) {
        if (COMMAND_WARM_STOP.equals(intent.getStringExtra(EXTRA_COMMAND))) {
            warmStop(intent.getStringExtra(EXTRA_CHANNEL_KEY));
            return;
        }

        String streamUrl = intent.getStringExtra(EXTRA_STREAM_URL);
        String title = intent.getStringExtra(EXTRA_TITLE);
        boolean drm = intent.getBooleanExtra(EXTRA_DRM, false);
        String licenseUrl = intent.getStringExtra(EXTRA_LICENSE_URL);
        boolean captions = intent.getBooleanExtra(EXTRA_CAPTIONS, true);

        if (streamUrl == null || streamUrl.isEmpty()) {
            Log.e(TAG, "no stream_url extra, finishing");
            finish();
            return;
        }

        // Applied per-trigger, not a live mid-stream toggle: the server bakes the current
        // FastChannels Player captions setting into this Intent extra at am-start time
        // (fc_player_bridge.trigger_channel), and this is the only moment this Activity
        // re-checks it. Covers both real sidecar subtitle renditions and muxed CEA-608/708
        // captions (see the trackSelector construction comment in onCreate).
        trackSelector.setParameters(trackSelector.buildUponParameters()
                .setTrackTypeDisabled(C.TRACK_TYPE_TEXT, !captions)
                .setPreferredTextLanguage(captions ? "en" : null)
                .setSelectUndeterminedTextLanguage(captions));

        Log.i(TAG, "playing \"" + title + "\" drm=" + drm + " url=" + streamUrl);
        // A fresh tune is a clean slate — cancel any retry left pending from whatever was
        // previously playing (its scheduled player.prepare() would otherwise race this one)
        // and don't carry its exhausted-or-not retry budget onto an unrelated channel.
        retryHandler.removeCallbacksAndMessages(null);
        retryCount = 0;
        liveGapStartedAtMs = 0;
        activeIsLive = false;
        activeChannelKey = intent.getStringExtra(EXTRA_CHANNEL_KEY);
        activeStreamUrl = streamUrl;
        activeDrm = drm;
        activeLicenseUrl = licenseUrl;
        reprepare();
        stallRecoveries = 0;
        startStallWatch();

        boundaryPollHandler.removeCallbacksAndMessages(null);
        Matcher slingMatch = SLING_DASH_URL_RE.matcher(streamUrl);
        if (drm && slingMatch.find()) {
            boundaryStatusUrl = slingMatch.group(1) + "/play/sling/" + slingMatch.group(2) + "/boundary-status";
            schedulePollBoundaryStatus(BOUNDARY_POLL_FAR_MS);
        } else {
            boundaryStatusUrl = null;
        }
    }

    /** Builds and sets a fresh MediaItem from the cached active* fields and prepares the
     * player. Shared by playFromIntent() (a real tune/retune) and the Lever 2 boundary-
     * confirmed swap (onBoundaryStatusResult) — both need the exact same construction, and
     * player.prepare() alone is a documented no-op unless the player is already
     * STATE_IDLE, so a genuine re-fetch of a changed manifest needs a fresh MediaItem, not
     * just another prepare() call. */
    private void reprepare() {
        MediaItem.Builder itemBuilder = new MediaItem.Builder().setUri(activeStreamUrl);
        if (activeDrm && activeLicenseUrl != null && !activeLicenseUrl.isEmpty()) {
            itemBuilder.setDrmConfiguration(
                    new MediaItem.DrmConfiguration.Builder(C.WIDEVINE_UUID)
                            .setLicenseUri(activeLicenseUrl)
                            .build());
        }
        player.setMediaItem(itemBuilder.build());
        player.prepare();
        player.setPlayWhenReady(true);
    }

    private void recordStatusEvent(String kind) {
        try {
            statusEvents.addLast(new JSONObject().put("t", System.currentTimeMillis()).put("kind", kind));
        } catch (JSONException ignored) {
        }
        while (statusEvents.size() > STATUS_MAX_EVENTS) statusEvents.removeFirst();
        persistStatusEvents();
    }

    private JSONObject buildStatus() {
        if (player == null) return null;
        JSONObject status = new JSONObject();
        try {
            status.put("updated_ms", System.currentTimeMillis());
            status.put("channel_key", activeChannelKey == null ? JSONObject.NULL : activeChannelKey);
            status.put("playing", player.isPlaying());
            VideoSize size = player.getVideoSize();
            if (size.width > 0) status.put("video_width", size.width).put("video_height", size.height);
            Format format = player.getVideoFormat();
            if (format != null && format.bitrate > 0) status.put("video_bitrate", format.bitrate);
            status.put("events", new JSONArray(statusEvents));
        } catch (JSONException e) {
            return null;
        }
        return status;
    }

    private void persistStatusEvents() {
        final File dir = getExternalFilesDir(null);
        if (dir == null) return;
        final String body = "{\"events\":" + new JSONArray(statusEvents) + "}";
        statusExecutor.execute(() -> {
            File tmp = new File(dir, STATUS_FILE + ".tmp");
            try (FileOutputStream out = new FileOutputStream(tmp)) {
                out.write(body.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                Log.w(TAG, "status write failed: " + e);
                return;
            }
            if (!tmp.renameTo(new File(dir, STATUS_FILE))) Log.w(TAG, "status rename failed");
        });
    }

    @Override
    public void dump(String prefix, FileDescriptor fd, PrintWriter writer, String[] args) {
        super.dump(prefix, fd, writer, args);
        JSONObject status = buildStatus();
        if (status != null) writer.println(prefix + STATUS_DUMP_PREFIX + status);
    }

    private void loadStatusEvents() {
        File dir = getExternalFilesDir(null);
        if (dir == null) return;
        File file = new File(dir, STATUS_FILE);
        if (!file.exists()) return;
        try (FileInputStream in = new FileInputStream(file)) {
            byte[] data = new byte[(int) file.length()];
            int read = in.read(data);
            JSONArray events = new JSONObject(new String(data, 0, Math.max(read, 0), StandardCharsets.UTF_8))
                    .optJSONArray("events");
            for (int i = 0; events != null && i < events.length(); i++) {
                statusEvents.addLast(events.getJSONObject(i));
            }
        } catch (IOException | JSONException e) {
            Log.w(TAG, "status load failed: " + e);
        }
    }

    private boolean isLiveGapError(PlaybackException error) {
        boolean live = activeIsLive || (player != null && player.isCurrentMediaItemLive());
        int code = error.errorCode;
        boolean ioError = code >= PlaybackException.ERROR_CODE_IO_UNSPECIFIED
                && code < PlaybackException.ERROR_CODE_PARSING_CONTAINER_MALFORMED;
        return live && (ioError || code == PlaybackException.ERROR_CODE_BEHIND_LIVE_WINDOW);
    }

    private void startStallWatch() {
        stallHandler.removeCallbacks(checkStall);
        lastStallPosMs = -1;
        lastAdvanceAtMs = System.currentTimeMillis();
        stallHandler.postDelayed(checkStall, STALL_CHECK_INTERVAL_MS);
    }

    private void checkStall() {
        if (player == null || activeStreamUrl == null) return;
        long now = System.currentTimeMillis();
        long pos = player.getCurrentPosition();
        boolean shouldBePlaying = player.getPlaybackState() == Player.STATE_READY && player.getPlayWhenReady();
        if (!shouldBePlaying || pos != lastStallPosMs) {
            lastStallPosMs = pos;
            lastAdvanceAtMs = now;
            if (stallRecoveries > 0 && shouldBePlaying
                    && now - lastStallRecoveryAtMs >= STALL_RECOVERY_RESET_AFTER_MS) {
                stallRecoveries = 0;
            }
        } else if (now - lastAdvanceAtMs >= STALL_THRESHOLD_MS) {
            if (stallRecoveries >= MAX_STALL_RECOVERIES) {
                Log.e(TAG, "playback frozen again after " + MAX_STALL_RECOVERIES
                        + " recoveries, giving up channel_key=" + activeChannelKey);
                recordStatusEvent("gave_up");
                finish();
                return;
            }
            stallRecoveries++;
            lastStallRecoveryAtMs = now;
            Log.w(TAG, "playback frozen " + (now - lastAdvanceAtMs) / 1000 + "s at position "
                    + pos + "ms while READY, rebuilding (" + stallRecoveries + "/"
                    + MAX_STALL_RECOVERIES + ") channel_key=" + activeChannelKey);
            reprepare();
            lastStallPosMs = -1;
            lastAdvanceAtMs = now;
            recordStatusEvent("freeze_rebuild");
        }
        stallHandler.postDelayed(checkStall, STALL_CHECK_INTERVAL_MS);
    }

    private void schedulePollBoundaryStatus(long delayMs) {
        if (boundaryPollRunnable != null) {
            boundaryPollHandler.removeCallbacks(boundaryPollRunnable);
        }
        boundaryPollRunnable = this::pollBoundaryStatus;
        boundaryPollHandler.postDelayed(boundaryPollRunnable, delayMs);
    }

    private void pollBoundaryStatus() {
        String url = boundaryStatusUrl;
        if (url == null) return;
        boundaryPollExecutor.execute(() -> {
            boolean confirmed = false;
            Double boundaryEpoch = null;
            HttpURLConnection conn = null;
            try {
                conn = (HttpURLConnection) new URL(url).openConnection();
                conn.setConnectTimeout(5000);
                conn.setReadTimeout(5000);
                StringBuilder sb = new StringBuilder();
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) sb.append(line);
                }
                JSONObject json = new JSONObject(sb.toString());
                confirmed = json.optBoolean("confirmed", false);
                if (!json.isNull("boundary_epoch")) {
                    boundaryEpoch = json.getDouble("boundary_epoch");
                }
            } catch (Exception e) {
                Log.w(TAG, "boundary-status poll failed: " + e);
            } finally {
                if (conn != null) conn.disconnect();
            }

            final boolean finalConfirmed = confirmed;
            final Double finalBoundaryEpoch = boundaryEpoch;
            boundaryPollHandler.post(() -> onBoundaryStatusResult(url, finalConfirmed, finalBoundaryEpoch));
        });
    }

    private void onBoundaryStatusResult(String pollUrl, boolean confirmed, Double boundaryEpoch) {
        // The channel could have changed (or stopped) while this poll was in flight on
        // the background thread — a stale response for a URL we've since moved on from
        // must not act on the current (unrelated) MediaItem.
        if (boundaryStatusUrl == null || !boundaryStatusUrl.equals(pollUrl)) return;

        if (confirmed) {
            Log.i(TAG, "boundary confirmed client-side for " + activeChannelKey + ", swapping locally");
            // Fire-and-forget: tells the server this boundary is already handled so its
            // own Lever 1 watchdog (independently polling the same underlying fact)
            // stands down instead of firing a redundant adb retune on top of this swap
            // — confirmed live 2026-09-15 that without this, both sides tend to confirm
            // within ~1s of each other and collide (see mark_boundary_handled_by_client's
            // docstring for why this narrows, but doesn't fully close, that race).
            ackBoundaryHandled(pollUrl);
            reprepare();
            schedulePollBoundaryStatus(BOUNDARY_POLL_FAR_MS);
            return;
        }

        long delayMs = BOUNDARY_POLL_FAR_MS;
        if (boundaryEpoch != null) {
            long secondsToBoundary = (long) (boundaryEpoch - (System.currentTimeMillis() / 1000.0));
            delayMs = secondsToBoundary <= BOUNDARY_NEAR_WINDOW_S
                    ? BOUNDARY_POLL_NEAR_MS
                    : Math.min(BOUNDARY_POLL_FAR_MS, (secondsToBoundary - BOUNDARY_NEAR_WINDOW_S) * 1000);
        }
        schedulePollBoundaryStatus(delayMs);
    }

    /** Best-effort POST to the paired boundary-ack endpoint — derived from the same
     * boundary-status URL this poll came from, since both live under the same
     * /play/sling/<id>/ path. Never blocks the caller and any failure is just logged;
     * worst case the server's own watchdog fires redundantly, same as before this
     * existed. */
    private void ackBoundaryHandled(String statusUrl) {
        String ackUrl = statusUrl.replace("/boundary-status", "/boundary-ack");
        boundaryPollExecutor.execute(() -> {
            HttpURLConnection conn = null;
            try {
                conn = (HttpURLConnection) new URL(ackUrl).openConnection();
                conn.setRequestMethod("POST");
                conn.setConnectTimeout(5000);
                conn.setReadTimeout(5000);
                conn.setDoOutput(false);
                conn.getResponseCode();
            } catch (Exception e) {
                Log.w(TAG, "boundary-ack failed: " + e);
            } finally {
                if (conn != null) conn.disconnect();
            }
        });
    }

    /** Best-effort POST telling the server this channel is no longer being watched —
     * see fc_player_bridge.clear_active_channel_tracking. Derives the server's base
     * URL from activeStreamUrl since that's already known for any source (not just
     * Sling's DRM shape, unlike boundaryStatusUrl which is sling-only). */
    private void ackWarmStop(String channelKey) {
        if (channelKey == null || channelKey.isEmpty() || activeStreamUrl == null) return;
        Matcher baseMatch = Pattern.compile("^(https?://[^/]+)").matcher(activeStreamUrl);
        if (!baseMatch.find()) return;
        String url = baseMatch.group(1) + "/play/fc-player/warm-stop-ack?channel_key="
                + Uri.encode(channelKey);
        boundaryPollExecutor.execute(() -> {
            HttpURLConnection conn = null;
            try {
                conn = (HttpURLConnection) new URL(url).openConnection();
                conn.setRequestMethod("POST");
                conn.setConnectTimeout(5000);
                conn.setReadTimeout(5000);
                conn.setDoOutput(false);
                conn.getResponseCode();
            } catch (Exception e) {
                Log.w(TAG, "warm-stop-ack failed: " + e);
            } finally {
                if (conn != null) conn.disconnect();
            }
        });
    }

    /**
     * Stop playback without killing the process. ah4c calls this as soon as its
     * downstream client disconnects; retaining the Activity, MediaSession, and
     * ExoPlayer lets the next adb launch take the warm onNewIntent() path.
     *
     * The channel key makes a delayed stop from an old ah4c tune harmless: once
     * a newer launch has claimed the Activity, it must not stop that new stream.
     */
    private void warmStop(String expectedChannelKey) {
        if (expectedChannelKey != null && !expectedChannelKey.isEmpty()
                && activeChannelKey != null && !expectedChannelKey.equals(activeChannelKey)) {
            Log.i(TAG, "ignoring stale warm stop for " + expectedChannelKey
                    + "; active=" + activeChannelKey);
            return;
        }

        Log.i(TAG, "warm-stopping " + (activeChannelKey == null ? "player" : activeChannelKey));
        retryHandler.removeCallbacksAndMessages(null);
        retryCount = 0;
        stallHandler.removeCallbacksAndMessages(null);
        boundaryPollHandler.removeCallbacksAndMessages(null);
        boundaryStatusUrl = null;
        // Tells the server nobody's watching this channel anymore, so its
        // block-boundary watchdog stops tracking it instead of firing pointless
        // retunes hours later — confirmed live 2026-09-15 that without this, a
        // stopped channel's next boundary still triggered a real adb relaunch.
        ackWarmStop(activeChannelKey);
        player.stop();
        player.clearMediaItems();
        activeChannelKey = null;
        // Stay foregrounded instead of moveTaskToBack(): backgrounding exposed the Fire TV
        // launcher for the entire gap until the next tune's am start arrives (1-2.5s observed
        // live), and doesn't speed up the retune.
    }

    @Override
    protected void onDestroy() {
        retryHandler.removeCallbacksAndMessages(null);
        stallHandler.removeCallbacksAndMessages(null);
        boundaryPollHandler.removeCallbacksAndMessages(null);
        boundaryStatusUrl = null;
        boundaryPollExecutor.shutdownNow();
        statusExecutor.shutdown();
        if (mediaSession != null) {
            mediaSession.release();
            mediaSession = null;
        }
        if (player != null) {
            player.release();
            player = null;
        }
        super.onDestroy();
    }
}
