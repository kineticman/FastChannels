package com.fastchannels.player;

import android.app.ActivityManager;
import android.content.Context;
import android.net.Uri;
import android.os.Build;
import android.os.Handler;
import android.os.Looper;
import android.util.Log;
import android.view.Display;

import androidx.media3.common.C;
import androidx.media3.common.Format;
import androidx.media3.common.Player;
import androidx.media3.common.util.UnstableApi;
import androidx.media3.exoplayer.ExoPlayer;
import androidx.media3.exoplayer.LoadControl;
import androidx.media3.exoplayer.analytics.AnalyticsListener;
import androidx.media3.exoplayer.source.LoadEventInfo;
import androidx.media3.exoplayer.source.MediaLoadData;
import androidx.media3.exoplayer.upstream.DefaultBandwidthMeter;

import java.util.Locale;
import java.util.function.Supplier;

/**
 * Buffer / load / live-edge diagnostics under their own tag (FCPlayer.Diag), added for the
 * ad-break freezes and the 1080p60 rebuffering seen on Hulu (issue #104). Answers, from the
 * device's own point of view, the questions the stock EventLogger can't:
 *
 *  - how much media is buffered ahead, how far behind the live edge the player is, and how
 *    much allocator memory the buffer holds (a tick every 5 s while a tune is active)
 *  - whether the loader is running or has been stopped by the load control (isLoading
 *    changes, with the buffer numbers at that moment: this is the "Target buffer size
 *    reached with less than 500ms" situation made visible once, instead of 100 times a second)
 *  - how long each media chunk really took to download on this device, and how fast
 *  - every rebuffer: what the buffer looked like when it started and how long it lasted
 *  - dropped frames, the decoder in use, the display refresh rate, heap and memory class
 *
 * Quiet at rest (one line per 5 s and per chunk) and it never touches playback.
 */
@UnstableApi
final class LoadDiagnostics implements AnalyticsListener {
    static final String TAG = "FCPlayer.Diag";
    private static final long TICK_MS = 5_000;
    private static final long SLOW_CHUNK_MS = 2_500;

    private final Context context;
    private final ExoPlayer player;
    private final LoadControl loadControl;
    private final DefaultBandwidthMeter bandwidth;
    private final Supplier<String> channelKey;
    private final Handler handler = new Handler(Looper.getMainLooper());

    private long bufferingSinceMs = 0;
    private long bufferingStartAheadMs = 0;
    private int rebuffers = 0;
    private long rebufferTotalMs = 0;
    private int droppedFrames = 0;
    private int slowChunks = 0;
    private long chunkCount = 0;
    private long lastVideoChunkMs = 0;
    private long tickCount = 0;

    private final Runnable tick = new Runnable() {
        @Override
        public void run() {
            logTick();
            handler.postDelayed(this, TICK_MS);
        }
    };

    LoadDiagnostics(Context context, ExoPlayer player, LoadControl loadControl,
                    DefaultBandwidthMeter bandwidth, Supplier<String> channelKey) {
        this.context = context;
        this.player = player;
        this.loadControl = loadControl;
        this.bandwidth = bandwidth;
        this.channelKey = channelKey;
        logDevice();
    }

    /** Called on every tune. Resets the per-tune counters and (re)starts the 5 s tick. */
    void onTune() {
        bufferingSinceMs = 0;
        rebuffers = 0;
        rebufferTotalMs = 0;
        droppedFrames = 0;
        slowChunks = 0;
        chunkCount = 0;
        tickCount = 0;
        handler.removeCallbacks(tick);
        handler.postDelayed(tick, TICK_MS);
        Log.i(TAG, "tune channel_key=" + key() + " " + display());
    }

    void release() {
        handler.removeCallbacksAndMessages(null);
    }

    private String key() {
        String k = channelKey.get();
        return k == null ? "-" : k;
    }

    private void logDevice() {
        try {
            ActivityManager am = (ActivityManager) context.getSystemService(Context.ACTIVITY_SERVICE);
            Runtime rt = Runtime.getRuntime();
            Log.i(TAG, "device model=" + Build.MODEL + " sdk=" + Build.VERSION.SDK_INT
                    + " lowRam=" + (am != null && am.isLowRamDevice())
                    + " memClassMB=" + (am == null ? -1 : am.getMemoryClass())
                    + " largeMemClassMB=" + (am == null ? -1 : am.getLargeMemoryClass())
                    + " maxHeapMB=" + rt.maxMemory() / (1024 * 1024)
                    + " cores=" + rt.availableProcessors());
        } catch (RuntimeException e) {
            Log.w(TAG, "device info failed: " + e);
        }
    }

    private String display() {
        try {
            android.view.WindowManager wm = (android.view.WindowManager) context.getSystemService(Context.WINDOW_SERVICE);
            Display d = wm.getDefaultDisplay();
            Display.Mode m = d.getMode();
            return String.format(Locale.US, "display=%dx%d@%.2fHz", m.getPhysicalWidth(), m.getPhysicalHeight(), m.getRefreshRate());
        } catch (RuntimeException e) {
            return "display=?";
        }
    }

    private Object cachedPlayerId;
    private boolean playerIdLooked;

    /** Media3 1.11 made the allocator per-player (LoadControl.getAllocator(PlayerId)) and
     * rejects a null/unset id, which an app has no public way to read; the player keeps it
     * in a field. Diagnostics only: any failure here just means "no allocator numbers". */
    private Object playerId() {
        if (!playerIdLooked) {
            playerIdLooked = true;
            try {
                java.lang.reflect.Field f = player.getClass().getDeclaredField("playerId");
                f.setAccessible(true);
                cachedPlayerId = f.get(player);
            } catch (Throwable t) {
                Log.w(TAG, "player id not readable, allocator numbers off: " + t);
            }
        }
        return cachedPlayerId;
    }

    /** Allocator bytes in KB, or -1 when it can't be read. Never throws. */
    private long allocatedKb() {
        try {
            Object id = playerId();
            if (id instanceof androidx.media3.exoplayer.analytics.PlayerId) {
                return loadControl.getAllocator((androidx.media3.exoplayer.analytics.PlayerId) id).getTotalBytesAllocated() / 1024;
            }
        } catch (Throwable t) {
            // fall through
        }
        return -1;
    }

    private long aheadMs() {
        return Math.max(0, player.getTotalBufferedDuration());
    }

    private String snapshot() {
        Runtime rt = Runtime.getRuntime();
        long liveOffset = player.getCurrentLiveOffset();
        Format v = player.getVideoFormat();
        return String.format(Locale.US,
                "ahead=%.1fs liveOffset=%s speed=%.2f loading=%s alloc=%dKB heap=%d/%dMB bw=%.1fMbps video=%s dropped=%d",
                aheadMs() / 1000.0,
                liveOffset == C.TIME_UNSET ? "-" : String.format(Locale.US, "%.1fs", liveOffset / 1000.0),
                player.getPlaybackParameters().speed,
                player.isLoading(),
                allocatedKb(),
                (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024), rt.maxMemory() / (1024 * 1024),
                bandwidth.getBitrateEstimate() / 1e6,
                v == null ? "-" : v.width + "x" + v.height + "@" + (v.frameRate > 0 ? String.format(Locale.US, "%.0f", v.frameRate) : "?")
                        + "/" + (v.bitrate > 0 ? v.bitrate / 1000 + "k" : "?"),
                droppedFrames);
    }

    private long lastHighLogMs = 0;

    /** One-off dump when the allocator is far above what a ~10 s buffer needs (a normal run
     * sits at 5-20 MB; one Hulu Lifetime run grew to 143 MB at the download rate while
     * playback carried on, then the load control stopped the loader). Logs where the
     * player thinks it is, so the cause can be read from the next occurrence. */
    private void logHighAllocator() {
        long kb = allocatedKb();
        long now = System.currentTimeMillis();
        if (kb < 60 * 1024 || now - lastHighLogMs < 30_000) return;  // also skips -1 (unreadable)
        lastHighLogMs = now;
        androidx.media3.common.Timeline tl = player.getCurrentTimeline();
        StringBuilder sb = new StringBuilder("ALLOCATOR HIGH ").append(kb / 1024).append("MB: ")
                .append("periodIdx=").append(player.getCurrentPeriodIndex()).append('/').append(tl.getPeriodCount())
                .append(" pos=").append(player.getCurrentPosition())
                .append(" bufferedPos=").append(player.getBufferedPosition())
                .append(" contentBufferedPos=").append(player.getContentBufferedPosition())
                .append(" totalBufferedMs=").append(player.getTotalBufferedDuration())
                .append(" duration=").append(player.getDuration())
                .append(" isPlayingAd=").append(player.isPlayingAd())
                .append(" isLoading=").append(player.isLoading());
        if (!tl.isEmpty()) {
            androidx.media3.common.Timeline.Window w = tl.getWindow(player.getCurrentMediaItemIndex(), new androidx.media3.common.Timeline.Window());
            sb.append(" windowStartDefault=").append(w.defaultPositionUs / 1000).append(" windowDur=").append(w.durationUs / 1000);
            androidx.media3.common.Timeline.Period pd = new androidx.media3.common.Timeline.Period();
            int n = tl.getPeriodCount();
            for (int i = Math.max(0, n - 3); i < n; i++) {
                tl.getPeriod(i, pd);
                sb.append(" [p").append(i).append(" dur=").append(pd.durationUs / 1000).append(']');
            }
        }
        Log.w(TAG, sb.toString());
    }

    private void logTick() {
        tickCount++;
        logHighAllocator();
        Log.i(TAG, "tick#" + tickCount + " state=" + stateName(player.getPlaybackState())
                + " pos=" + player.getCurrentPosition() / 1000 + "s " + snapshot()
                + " rebuffers=" + rebuffers + "(" + rebufferTotalMs + "ms) slowChunks=" + slowChunks
                + "/" + chunkCount);
    }

    private static String stateName(int s) {
        switch (s) {
            case Player.STATE_IDLE: return "IDLE";
            case Player.STATE_BUFFERING: return "BUFFERING";
            case Player.STATE_READY: return "READY";
            case Player.STATE_ENDED: return "ENDED";
            default: return String.valueOf(s);
        }
    }

    @Override
    public void onPlaybackStateChanged(EventTime eventTime, int state) {
        long now = System.currentTimeMillis();
        if (state == Player.STATE_BUFFERING) {
            if (bufferingSinceMs == 0) {
                bufferingSinceMs = now;
                bufferingStartAheadMs = aheadMs();
                rebuffers++;
                Log.w(TAG, "BUFFERING start #" + rebuffers + " " + snapshot());
            }
        } else if (bufferingSinceMs != 0) {
            long took = now - bufferingSinceMs;
            rebufferTotalMs += took;
            Log.w(TAG, "BUFFERING end after " + took + "ms (ahead was "
                    + bufferingStartAheadMs + "ms at start) -> " + stateName(state) + " " + snapshot());
            bufferingSinceMs = 0;
        }
    }

    /** The load control stopping or restarting the loader, with the numbers that decided it. */
    @Override
    public void onIsLoadingChanged(EventTime eventTime, boolean isLoading) {
        Log.i(TAG, "loader " + (isLoading ? "RUNNING" : "STOPPED") + " " + snapshot());
    }

    @Override
    public void onLoadCompleted(EventTime eventTime, LoadEventInfo info, MediaLoadData data) {
        if (data.dataType != C.DATA_TYPE_MEDIA && data.dataType != C.DATA_TYPE_MANIFEST) return;
        boolean manifest = data.dataType == C.DATA_TYPE_MANIFEST;
        long ms = info.loadDurationMs;
        long kb = info.bytesLoaded / 1024;
        double mbps = ms > 0 ? info.bytesLoaded * 8.0 / ms / 1000.0 : 0;
        String kind = manifest ? "manifest" : (data.trackType == C.TRACK_TYPE_VIDEO ? "video"
                : data.trackType == C.TRACK_TYPE_AUDIO ? "audio" : "other");
        if (!manifest) chunkCount++;
        boolean slow = ms >= SLOW_CHUNK_MS;
        if (slow && !manifest) slowChunks++;
        // Video chunks are the interesting ones; log each one, audio/manifest only when slow.
        if (manifest && !slow) return;
        if (kind.equals("audio") && !slow) return;
        Uri u = info.uri;
        long gap = 0;
        if (kind.equals("video")) {
            long now = System.currentTimeMillis();
            gap = lastVideoChunkMs == 0 ? 0 : now - lastVideoChunkMs;
            lastVideoChunkMs = now;
        }
        Log.i(TAG, "load " + kind + (slow ? " SLOW" : "") + " " + kb + "KB in " + ms + "ms ("
                + String.format(Locale.US, "%.1f", mbps) + "Mbps) host=" + (u == null ? "?" : u.getHost())
                + (data.trackFormat != null && data.trackFormat.bitrate > 0 ? " rep=" + data.trackFormat.bitrate / 1000 + "k" : "")
                + (gap > 0 ? " sincePrevVideoChunk=" + gap + "ms" : "")
                + " ahead=" + String.format(Locale.US, "%.1f", aheadMs() / 1000.0) + "s");
    }

    @Override
    public void onDroppedVideoFrames(EventTime eventTime, int dropped, long elapsedMs) {
        droppedFrames += dropped;
        Log.w(TAG, "dropped " + dropped + " frames in " + elapsedMs + "ms (total " + droppedFrames + ") "
                + snapshot());
    }
}
