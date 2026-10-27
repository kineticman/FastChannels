package com.fastchannels.player;

import android.util.Log;

import androidx.media3.common.C;
import androidx.media3.common.Format;
import androidx.media3.common.Tracks;
import androidx.media3.common.util.UnstableApi;
import androidx.media3.exoplayer.DecoderReuseEvaluation;
import androidx.media3.exoplayer.analytics.AnalyticsListener;
import androidx.media3.exoplayer.audio.AudioSink;

import java.util.function.Supplier;

/**
 * Logs what happens to the audio under its own tag (FCPlayer.Audio), so audio complaints
 * on bridged channels — clicks at the start, silence at an ad break — can be matched to
 * what the player actually did: which audio rendition it picked, when the format changed
 * mid-stream, whether the output ran dry (underrun), and any decoder or output errors.
 * FastChannels' bridge watchdog copies these lines into its own log when Debug logging is
 * on. Quiet by design: a handful of lines per tune, underruns rate-limited.
 */
@UnstableApi
final class AudioDiagnostics implements AnalyticsListener {
    static final String TAG = "FCPlayer.Audio";
    private static final long UNDERRUN_LOG_INTERVAL_MS = 5000;

    private final Supplier<String> channelKey;
    private long lastUnderrunLogMs = 0;
    private int underrunsSinceLog = 0;

    AudioDiagnostics(Supplier<String> channelKey) {
        this.channelKey = channelKey;
    }

    private String key() {
        String k = channelKey.get();
        return k == null ? "-" : k;
    }

    @Override
    public void onTracksChanged(EventTime eventTime, Tracks tracks) {
        StringBuilder available = new StringBuilder();
        for (Tracks.Group group : tracks.getGroups()) {
            if (group.getType() != C.TRACK_TYPE_AUDIO) continue;
            for (int i = 0; i < group.length; i++) {
                if (available.length() > 0) available.append(", ");
                available.append(describe(group.getTrackFormat(i)));
                if (group.isTrackSelected(i)) available.append(" [selected]");
            }
        }
        if (available.length() > 0) {
            Log.i(TAG, "audio tracks channel_key=" + key() + ": " + available);
        }
    }

    @Override
    public void onAudioInputFormatChanged(EventTime eventTime, Format format,
                                          DecoderReuseEvaluation evaluation) {
        Log.i(TAG, "audio format channel_key=" + key() + ": " + describe(format));
    }

    @Override
    public void onAudioDecoderInitialized(EventTime eventTime, String decoderName,
                                          long initializedTimestampMs, long initializationDurationMs) {
        Log.i(TAG, "audio decoder channel_key=" + key() + ": " + decoderName
                + " (" + initializationDurationMs + "ms)");
    }

    @Override
    public void onAudioTrackInitialized(EventTime eventTime,
                                        AudioSink.AudioTrackConfig config) {
        Log.i(TAG, "audio output channel_key=" + key() + ": encoding=" + config.encoding
                + " " + config.sampleRate + "Hz channelMask=0x" + Integer.toHexString(config.channelConfig)
                + " offload=" + config.offload + " tunneling=" + config.tunneling);
    }

    @Override
    public void onAudioUnderrun(EventTime eventTime, int bufferSize, long bufferSizeMs,
                                long elapsedSinceLastFeedMs) {
        underrunsSinceLog++;
        long now = System.currentTimeMillis();
        if (now - lastUnderrunLogMs < UNDERRUN_LOG_INTERVAL_MS) return;
        Log.w(TAG, "audio underrun channel_key=" + key() + ": " + underrunsSinceLog
                + " since last report, buffer " + bufferSizeMs + "ms, last fed "
                + elapsedSinceLastFeedMs + "ms ago");
        lastUnderrunLogMs = now;
        underrunsSinceLog = 0;
    }

    @Override
    public void onAudioSinkError(EventTime eventTime, Exception audioSinkError) {
        Log.w(TAG, "audio output error channel_key=" + key() + ": " + audioSinkError);
    }

    @Override
    public void onAudioCodecError(EventTime eventTime, Exception audioCodecError) {
        Log.w(TAG, "audio decoder error channel_key=" + key() + ": " + audioCodecError);
    }

    private static String describe(Format f) {
        StringBuilder s = new StringBuilder();
        s.append(f.sampleMimeType != null ? f.sampleMimeType : String.valueOf(f.codecs));
        if (f.channelCount != Format.NO_VALUE) s.append(' ').append(f.channelCount).append("ch");
        if (f.sampleRate != Format.NO_VALUE) s.append(' ').append(f.sampleRate).append("Hz");
        if (f.bitrate != Format.NO_VALUE) s.append(' ').append(f.bitrate / 1000).append("kbps");
        if (f.language != null) s.append(' ').append(f.language);
        if (f.label != null) s.append(" \"").append(f.label).append('"');
        return s.toString();
    }
}
