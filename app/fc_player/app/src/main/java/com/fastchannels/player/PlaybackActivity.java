package com.fastchannels.player;

import android.app.Activity;
import android.os.Bundle;
import android.util.Log;
import android.view.WindowManager;

import androidx.media3.common.C;
import androidx.media3.common.MediaItem;
import androidx.media3.common.util.UnstableApi;
import androidx.media3.exoplayer.ExoPlayer;
import androidx.media3.ui.PlayerView;

/**
 * The entire app. Full-screen playback for FastChannels' remote-play trigger
 * (app/fc_player_bridge.py, the analog of kodi_bridge.trigger_channel()) — launched via
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

    private ExoPlayer player;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        getWindow().addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON
                | WindowManager.LayoutParams.FLAG_FULLSCREEN);

        String streamUrl = getIntent().getStringExtra(EXTRA_STREAM_URL);
        String title = getIntent().getStringExtra(EXTRA_TITLE);
        boolean drm = getIntent().getBooleanExtra(EXTRA_DRM, false);
        String licenseUrl = getIntent().getStringExtra(EXTRA_LICENSE_URL);

        if (streamUrl == null || streamUrl.isEmpty()) {
            Log.e(TAG, "no stream_url extra, finishing");
            finish();
            return;
        }

        PlayerView playerView = new PlayerView(this);
        setContentView(playerView);

        player = new ExoPlayer.Builder(this).build();
        playerView.setPlayer(player);
        playerView.setKeepScreenOn(true);

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
        if (player != null) {
            player.release();
            player = null;
        }
        super.onDestroy();
    }
}
