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
import androidx.media3.common.MediaItem;
import androidx.media3.common.PlaybackException;
import androidx.media3.common.Player;
import androidx.media3.common.util.UnstableApi;
import androidx.media3.datasource.DefaultHttpDataSource;
import androidx.media3.exoplayer.ExoPlayer;
import androidx.media3.exoplayer.source.DefaultMediaSourceFactory;
import androidx.media3.exoplayer.trackselection.DefaultTrackSelector;
import androidx.media3.exoplayer.upstream.DefaultBandwidthMeter;
import androidx.media3.exoplayer.util.EventLogger;
import androidx.media3.session.MediaSession;
import androidx.media3.ui.PlayerView;

import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
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
                Log.e(TAG, "playback error channel_key=" + activeChannelKey
                        + " (retry " + retryCount + "/" + MAX_RETRIES + "): " + error, error);
                retryHandler.removeCallbacks(resetRetryCount);
                if (retryCount < MAX_RETRIES) {
                    retryCount++;
                    retryHandler.postDelayed(() -> {
                        Log.i(TAG, "retrying playback channel_key=" + activeChannelKey);
                        player.prepare();
                    }, RETRY_DELAY_MS);
                } else {
                    Log.e(TAG, "giving up after " + MAX_RETRIES + " retries channel_key=" + activeChannelKey);
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
                    retryHandler.postDelayed(resetRetryCount, RETRY_RESET_AFTER_MS);
                } else {
                    retryHandler.removeCallbacks(resetRetryCount);
                }
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
        activeChannelKey = intent.getStringExtra(EXTRA_CHANNEL_KEY);
        activeStreamUrl = streamUrl;
        activeDrm = drm;
        activeLicenseUrl = licenseUrl;
        reprepare();

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
        boundaryPollHandler.removeCallbacksAndMessages(null);
        boundaryStatusUrl = null;
        boundaryPollExecutor.shutdownNow();
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
