package com.fastchannels.player;

import android.content.Context;

import androidx.media3.common.Format;
import androidx.media3.common.MimeTypes;
import androidx.media3.common.util.UnstableApi;
import androidx.media3.exoplayer.DefaultRenderersFactory;
import androidx.media3.exoplayer.audio.AudioSink;
import androidx.media3.exoplayer.audio.ForwardingAudioSink;

/**
 * DefaultRenderersFactory whose audio output can be told to refuse encoded audio
 * (AC3/E-AC3 and the like), so the renderer decodes it on the device and sends PCM
 * instead of passing the bitstream through to HDMI. Set per tune from FastChannels'
 * "Bridge audio" setting: HDMI capture cards that only handle stereo PCM can still tell
 * the stick they accept Dolby, and then click or drop out on it. The flag is read when
 * tracks are selected, so changing it takes effect on the next tune without rebuilding
 * the player.
 */
@UnstableApi
final class AudioModeRenderersFactory extends DefaultRenderersFactory {
    private volatile boolean pcmOnly = false;

    AudioModeRenderersFactory(Context context) {
        super(context);
    }

    boolean isPcmOnly() {
        return pcmOnly;
    }

    void setPcmOnly(boolean pcmOnly) {
        this.pcmOnly = pcmOnly;
    }

    @Override
    protected AudioSink buildAudioSink(Context context, boolean enableFloatOutput,
                                       boolean enableAudioTrackPlaybackParams) {
        AudioSink sink = super.buildAudioSink(context, enableFloatOutput, enableAudioTrackPlaybackParams);
        if (sink == null) return null;
        return new ForwardingAudioSink(sink) {
            @Override
            public boolean supportsFormat(Format format) {
                return !refuses(format) && super.supportsFormat(format);
            }

            @Override
            public int getFormatSupport(Format format) {
                return refuses(format) ? SINK_FORMAT_UNSUPPORTED : super.getFormatSupport(format);
            }
        };
    }

    private boolean refuses(Format format) {
        return pcmOnly && !MimeTypes.AUDIO_RAW.equals(format.sampleMimeType);
    }
}
