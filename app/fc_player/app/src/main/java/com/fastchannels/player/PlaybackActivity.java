package com.fastchannels.player;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.util.Log;
import android.view.WindowManager;

import androidx.media3.common.C;
import androidx.media3.common.MediaItem;
import androidx.media3.common.util.UnstableApi;
import androidx.media3.datasource.DefaultHttpDataSource;
import androidx.media3.exoplayer.ExoPlayer;
import androidx.media3.exoplayer.source.DefaultMediaSourceFactory;
import androidx.media3.exoplayer.trackselection.DefaultTrackSelector;
import androidx.media3.exoplayer.upstream.DefaultBandwidthMeter;
import androidx.media3.session.MediaSession;
import androidx.media3.ui.PlayerView;

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

    static final String EXTRA_STREAM_URL = "stream_url";
    static final String EXTRA_TITLE = "title";
    static final String EXTRA_DRM = "drm";
    static final String EXTRA_LICENSE_URL = "license_url";
    static final String EXTRA_CAPTIONS = "captions";

    private ExoPlayer player;
    private DefaultTrackSelector trackSelector;
    private MediaSession mediaSession;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
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
        playerView.setPlayer(player);
        playerView.setKeepScreenOn(true);

        // No lock-screen/notification controls needed — this device has no on-device UI
        // (see class docstring) — just publishing PlaybackState to dumpsys media_session so
        // an ah4c-driven tuner's stock is_media_playing() check (adb shell dumpsys
        // media_session, looking for state=PlaybackState {state=3}) can see this player the
        // same way it already sees Hulu/YouTube TV, instead of always reading "not playing".
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

        MediaItem.Builder itemBuilder = new MediaItem.Builder().setUri(streamUrl);
        if (drm && licenseUrl != null && !licenseUrl.isEmpty()) {
            itemBuilder.setDrmConfiguration(
                    new MediaItem.DrmConfiguration.Builder(C.WIDEVINE_UUID)
                            .setLicenseUri(licenseUrl)
                            .build());
        }

        Log.i(TAG, "playing \"" + title + "\" drm=" + drm + " url=" + streamUrl);
        player.setMediaItem(itemBuilder.build());
        player.prepare();
        player.setPlayWhenReady(true);
    }

    @Override
    protected void onDestroy() {
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
