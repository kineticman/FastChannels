package com.fastchannels.streamvault.bridge;

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
 * Full-screen playback for FastChannels' remote-play trigger (app/streamvault_bridge.py,
 * the analog of kodi_bridge.trigger_channel()) — launched via `adb shell am start` with plain
 * Intent extras (no cross-process Serializable involved; see streamvault_bridge.py's docstring
 * for why adb-shell-privileged launch is the mechanism that actually works here).
 *
 * Plays the resolved FastChannels URL directly with our own Media3/ExoPlayer instance rather
 * than routing through StreamVault at all. This exists because the natural approach — resolve
 * StreamVault's own TvContract channel row and fire a standard TV Input Framework tune Intent
 * (content://android.media.tv/channel/<id>, handled on this device by
 * com.amazon.tv.livetv/.TvChannelsPlayerActivityAlias, confirmed live) — requires
 * android.permission.READ_TV_LISTINGS to read that row id back, and Android silently never
 * actually grants that permission to a third-party app on this device (confirmed live
 * 2026-08-24 via dumpsys: declared in the manifest, absent from the granted install
 * permissions list). Playing it ourselves sidesteps that wall entirely, and reuses the exact
 * HLS/DASH/Widevine shape already validated live tonight (real secure decode via
 * StreamVaultTvInputService's own Media3 pipeline) — MediaItem.DrmConfiguration +
 * DefaultMediaSourceFactory's extension-based HLS/DASH auto-detection (our URLs always end in
 * .m3u8 or .mpd) means no manual MediaSource.Factory selection is needed here either.
 */
@UnstableApi
public class PlaybackActivity extends Activity {
    private static final String TAG = "FCBridge.Playback";

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
