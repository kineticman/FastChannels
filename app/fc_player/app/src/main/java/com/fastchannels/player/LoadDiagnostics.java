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
import androidx.media3.exoplayer.DecoderCounters;
import androidx.media3.exoplayer.ExoPlayer;
import androidx.media3.exoplayer.Renderer;
import androidx.media3.exoplayer.mediacodec.MediaCodecRenderer;
import androidx.media3.exoplayer.LoadControl;
import androidx.media3.exoplayer.analytics.AnalyticsListener;
import androidx.media3.exoplayer.source.LoadEventInfo;
import androidx.media3.exoplayer.source.MediaLoadData;
import androidx.media3.exoplayer.upstream.DefaultBandwidthMeter;

import java.lang.reflect.Field;
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
        tuneSummaryLogged = false;
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

    private boolean tuneSummaryLogged = false;
    private long lastVideoOut = -1;
    private long lastAudioOut = -1;
    private int lastPeriodCount = -1;

    /** Frames the video / audio decoders have delivered since the previous tick, so a
     * decoder that has gone quiet shows as +0 even when ExoPlayer still says READY. */
    private String decoderProgress() {
        long v = outputCount(player.getVideoDecoderCounters());
        long a = outputCount(player.getAudioDecoderCounters());
        String out = "out(v+" + (lastVideoOut < 0 || v < 0 ? "?" : (v - lastVideoOut))
                + " a+" + (lastAudioOut < 0 || a < 0 ? "?" : (a - lastAudioOut)) + ")";
        lastVideoOut = v;
        lastAudioOut = a;
        return out;
    }

    private static long outputCount(DecoderCounters c) {
        if (c == null) return -1;
        c.ensureUpdated();
        return c.renderedOutputBufferCount + c.skippedOutputBufferCount + c.droppedBufferCount;
    }

    /** Where the player is in the Period list, for tracing boundaries. */
    private String where() {
        try {
            return "p=" + player.getCurrentPeriodIndex() + "/" + player.getCurrentTimeline().getPeriodCount()
                    + " pos=" + player.getCurrentPosition() / 1000 + "s ahead="
                    + String.format(Locale.US, "%.1fs", aheadMs() / 1000.0);
        } catch (RuntimeException e) {
            return "p=?";
        }
    }

    private void trace(String what) {
        Log.i(TAG, "boundary " + what + " [" + where() + "]");
    }

    @Override
    public void onTimelineChanged(EventTime eventTime, int reason) {
        int n = eventTime.timeline.getPeriodCount();
        if (n != lastPeriodCount) {
            trace("periods " + lastPeriodCount + " -> " + n);
            lastPeriodCount = n;
        }
    }

    @Override
    public void onPositionDiscontinuity(EventTime eventTime, Player.PositionInfo oldPosition,
                                        Player.PositionInfo newPosition, int reason) {
        String r = reason == Player.DISCONTINUITY_REASON_AUTO_TRANSITION ? "AUTO_TRANSITION"
                : reason == Player.DISCONTINUITY_REASON_SEEK ? "SEEK"
                : reason == Player.DISCONTINUITY_REASON_REMOVE ? "REMOVE"
                : reason == Player.DISCONTINUITY_REASON_INTERNAL ? "INTERNAL"
                : reason == Player.DISCONTINUITY_REASON_SKIP ? "SKIP" : String.valueOf(reason);
        trace("discontinuity " + r + " period " + oldPosition.periodIndex + " -> " + newPosition.periodIndex
                + " pos " + oldPosition.positionMs / 1000 + "s -> " + newPosition.positionMs / 1000 + "s");
    }

    @Override public void onVideoEnabled(EventTime e, DecoderCounters c) { trace("video renderer ENABLED"); }
    @Override public void onVideoDisabled(EventTime e, DecoderCounters c) { trace("video renderer DISABLED"); }
    @Override public void onAudioEnabled(EventTime e, DecoderCounters c) { trace("audio renderer ENABLED"); }
    @Override public void onAudioDisabled(EventTime e, DecoderCounters c) { trace("audio renderer DISABLED"); }

    @Override
    public void onVideoDecoderInitialized(EventTime e, String name, long initializedMs, long durationMs) {
        trace("video decoder init " + name + " (" + durationMs + "ms)");
    }

    @Override public void onVideoDecoderReleased(EventTime e, String name) { trace("video decoder released " + name); }

    @Override
    public void onAudioDecoderInitialized(EventTime e, String name, long initializedMs, long durationMs) {
        trace("audio decoder init " + name + " (" + durationMs + "ms)");
    }

    @Override public void onAudioDecoderReleased(EventTime e, String name) { trace("audio decoder released " + name); }

    @Override
    public void onAudioUnderrun(EventTime e, int bufferSize, long bufferSizeMs, long elapsedSinceLastFeedMs) {
        trace("AUDIO UNDERRUN lastFeed=" + elapsedSinceLastFeedMs + "ms ago bufferMs=" + bufferSizeMs);
    }

    @Override public void onAudioSinkError(EventTime e, Exception err) { trace("AUDIO SINK ERROR " + err); }
    @Override public void onAudioCodecError(EventTime e, Exception err) { trace("AUDIO CODEC ERROR " + err); }
    @Override public void onVideoCodecError(EventTime e, Exception err) { trace("VIDEO CODEC ERROR " + err); }
    @Override public void onDrmSessionAcquired(EventTime e, int state) { trace("drm session acquired state=" + state); }
    @Override public void onDrmKeysLoaded(EventTime e) { trace("drm keys loaded"); }
    @Override public void onDrmKeysRestored(EventTime e) { trace("drm keys restored"); }
    @Override public void onDrmKeysRemoved(EventTime e) { trace("drm keys REMOVED"); }
    @Override public void onDrmSessionReleased(EventTime e) { trace("drm session released"); }
    @Override public void onDrmSessionManagerError(EventTime e, Exception err) { trace("DRM ERROR " + err); }

    @Override
    public void onRenderedFirstFrame(EventTime e, Object output, long renderTimeMs) {
        trace("first frame rendered");
        if (!tuneSummaryLogged) {
            tuneSummaryLogged = true;
            dumpRendererState("tune summary");
        }
    }

    /**
     * Snapshot of every renderer's state, for a stall. ExoPlayer only leaves a Period when
     * each renderer has read the old stream to its end and then is in the right state for
     * the new one; the log shows which renderer is the one holding it back. The renderer
     * getters are plain reads (racy from the main thread, fine for diagnostics); the
     * MediaCodec flags are private, read by reflection, and skipped if unavailable.
     */
    void dumpRendererState(String why) {
        try {
            StringBuilder sb = new StringBuilder("RENDERER STATE (" + why + ") " + where() + "\n");
            for (int i = 0; i < player.getRendererCount(); i++) {
                Renderer r = player.getRenderer(i);
                sb.append("  r").append(i).append(' ').append(typeName(r.getTrackType())).append(' ').append(r.getName())
                        .append(" state=").append(r.getState())
                        .append(" readToEnd=").append(r.hasReadStreamToEnd())
                        .append(" ended=").append(r.isEnded())
                        .append(" ready=").append(r.isReady())
                        .append(" readingPosMs=").append(r.getReadingPositionUs() == C.TIME_END_OF_SOURCE ? "END" : r.getReadingPositionUs() / 1000)
                        .append(" stream=").append(r.getStream() == null ? "none" : Integer.toHexString(System.identityHashCode(r.getStream())));
                if (r instanceof MediaCodecRenderer) {
                    for (String f : new String[]{"inputStreamEnded", "outputStreamEnded", "codecReceivedEos",
                            "codecReceivedBuffers", "codecDrainState", "codecDrainAction", "codecNeedsEosPropagation"}) {
                        try {
                            Field fld = MediaCodecRenderer.class.getDeclaredField(f);
                            fld.setAccessible(true);
                            sb.append(' ').append(f).append('=').append(fld.get(r));
                        } catch (Throwable ignored) {
                        }
                    }
                }
                sb.append('\n');
            }
            sb.append("  player: state=").append(stateName(player.getPlaybackState()))
                    .append(" playWhenReady=").append(player.getPlayWhenReady())
                    .append(" isPlaying=").append(player.isPlaying())
                    .append(" bufferedPosMs=").append(player.getBufferedPosition())
                    .append(" contentPosMs=").append(player.getContentPosition())
                    .append(" isLoading=").append(player.isLoading());
            Log.w(TAG, sb.toString());
        } catch (Throwable t) {
            Log.w(TAG, "renderer dump failed: " + t);
        }
    }

    private void logTick() {
        tickCount++;
        logHighAllocator();
        Log.i(TAG, "tick#" + tickCount + " state=" + stateName(player.getPlaybackState())
                + " pos=" + player.getCurrentPosition() / 1000 + "s " + where() + " " + decoderProgress() + " " + snapshot()
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

    /** Index of the Period an event belongs to, or -1. */
    private static int periodIdx(EventTime e) {
        try {
            if (e.mediaPeriodId == null) return -1;
            int i = e.timeline.getIndexOfPeriod(e.mediaPeriodId.periodUid);
            return i == C.INDEX_UNSET ? -1 : i;
        } catch (RuntimeException ex) {
            return -1;
        }
    }

    private final int[] lastLoadPeriod = {-2, -2};   // [video, audio]: Period of the newest chunk

    private static String typeName(int t) {
        return t == C.TRACK_TYPE_VIDEO ? "video" : t == C.TRACK_TYPE_AUDIO ? "audio"
                : t == C.TRACK_TYPE_METADATA ? "metadata" : t == C.TRACK_TYPE_TEXT ? "text" : "t" + t;
    }

    /** Each renderer going ready / not ready, tagged with the Period the player is reading.
     * At a healthy boundary the renderers go not-ready briefly and ready again in the new
     * Period; at a stalled one they stay not ready. */
    @Override
    public void onRendererReadyChanged(EventTime e, int rendererIndex, int trackType, boolean isReady) {
        if (trackType != C.TRACK_TYPE_VIDEO && trackType != C.TRACK_TYPE_AUDIO) return;
        trace(typeName(trackType) + " renderer " + (isReady ? "READY" : "NOT READY") + " p=" + periodIdx(e));
    }

    @Override
    public void onAudioPositionAdvancing(EventTime e, long playoutStartSystemTimeMs) {
        trace("audio position ADVANCING");
    }

    @Override
    public void onAudioTrackInitialized(EventTime e, androidx.media3.exoplayer.audio.AudioSink.AudioTrackConfig config) {
        trace("audio track initialized");
    }

    @Override
    public void onAudioTrackReleased(EventTime e, androidx.media3.exoplayer.audio.AudioSink.AudioTrackConfig config) {
        trace("audio track released");
    }

    @Override
    public void onDownstreamFormatChanged(EventTime e, MediaLoadData data) {
        if (data.trackType != C.TRACK_TYPE_VIDEO && data.trackType != C.TRACK_TYPE_AUDIO) return;
        Format f = data.trackFormat;
        trace("downstream format " + (data.trackType == C.TRACK_TYPE_VIDEO ? "video" : "audio") + " p=" + periodIdx(e)
                + (f == null ? "" : " " + f.sampleMimeType + (f.width > 0 ? " " + f.width + "x" + f.height : "")
                + (f.bitrate > 0 ? " " + f.bitrate / 1000 + "k" : "")));
    }

    @Override
    public void onLoadError(EventTime e, LoadEventInfo info, MediaLoadData data, java.io.IOException error,
                            boolean wasCanceled) {
        Log.w(TAG, "load ERROR " + (wasCanceled ? "(canceled) " : "") + "type=" + data.dataType
                + " track=" + data.trackType + " p=" + periodIdx(e) + " host="
                + (info.uri == null ? "?" : info.uri.getHost()) + " " + error + " [" + where() + "]");
    }

    @Override
    public void onLoadCompleted(EventTime eventTime, LoadEventInfo info, MediaLoadData data) {
        // First chunk (init segment or media) of each Period, per track: shows whether the
        // next Period's data arrived before the playhead did.
        if ((data.dataType == C.DATA_TYPE_MEDIA || data.dataType == C.DATA_TYPE_MEDIA_INITIALIZATION)
                && (data.trackType == C.TRACK_TYPE_VIDEO || data.trackType == C.TRACK_TYPE_AUDIO)) {
            int slot = data.trackType == C.TRACK_TYPE_VIDEO ? 0 : 1;
            int pi = periodIdx(eventTime);
            if (pi != lastLoadPeriod[slot]) {
                lastLoadPeriod[slot] = pi;
                trace("first " + (data.dataType == C.DATA_TYPE_MEDIA_INITIALIZATION ? "init" : "media") + " chunk of p=" + pi
                        + " for " + (slot == 0 ? "video" : "audio") + " (" + info.bytesLoaded / 1024 + "KB in "
                        + info.loadDurationMs + "ms)");
            }
        }
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
        Log.i(TAG, "load " + kind + " p=" + periodIdx(eventTime) + (slow ? " SLOW" : "") + " " + kb + "KB in " + ms + "ms ("
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
