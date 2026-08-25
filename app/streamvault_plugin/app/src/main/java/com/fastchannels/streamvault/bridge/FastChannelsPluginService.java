package com.fastchannels.streamvault.bridge;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.Service;
import android.content.Intent;
import android.content.pm.ServiceInfo;
import android.os.Build;
import android.os.Bundle;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.IBinder;
import android.os.Message;
import android.os.Messenger;
import android.os.RemoteException;
import android.util.Log;

import org.json.JSONObject;

/**
 * StreamVault plugin entry point. Bound Messenger service, discovered via the
 * com.streamvault.plugin.API intent-filter + MANIFEST_JSON meta-data in AndroidManifest.xml.
 *
 * Message envelope (request and reply) mirrors StreamVaultAdaptiveBridgePluginService from
 * github.com/jopsis/StreamVault-IPTV-Plugin-Adaptive-Bridge, read directly rather than
 * guessed: reply is Message.obtain(null, request.what) with a Bundle carrying api_version +
 * the echoed request_id, sent back via request.replyTo.send(reply).
 */
public class FastChannelsPluginService extends Service {
    private static final String TAG = "FCBridge.Service";
    private static final String NOTIFICATION_CHANNEL_ID = "fastchannels_bridge_sync";
    private static final int NOTIFICATION_ID = 93781;

    private HandlerThread handlerThread;
    private Messenger messenger;
    private ConfigStore configStore;
    private LocalPlaylistServer localServer;

    @Override
    public void onCreate() {
        super.onCreate();
        Log.i(TAG, "onCreate");
        configStore = new ConfigStore(this);
        localServer = new LocalPlaylistServer(configStore);
        localServer.start();
        handlerThread = new HandlerThread("fastchannels-bridge-plugin");
        handlerThread.start();
        messenger = new Messenger(new Handler(handlerThread.getLooper(), this::handleMessage));
        keepAlive();
        Log.i(TAG, "onCreate done");
    }

    @Override
    public IBinder onBind(Intent intent) {
        Log.i(TAG, "onBind");
        keepAlive();
        return messenger.getBinder();
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        Log.i(TAG, "onStartCommand");
        localServer.start();
        promoteToForeground();
        return START_STICKY;
    }

    @Override
    public void onDestroy() {
        Log.i(TAG, "onDestroy");
        localServer.stop();
        if (handlerThread != null) {
            handlerThread.quitSafely();
        }
        super.onDestroy();
    }

    /**
     * A bound-only service dies as soon as its last client unbinds — which happens right
     * after every short-lived discovery/config IPC call StreamVault makes. But StreamVault's
     * own M3U sync worker fetches the provider.m3u playlist via a plain HTTP client, outside
     * any Messenger binding, sometime later — so our local server has to keep running
     * independently of that bind/unbind churn or the fetch finds nothing listening.
     * Self-starting (not just binding) promotes this to a started+foreground service Android
     * won't tear down between binds. Confirmed live 2026-08-24: without this, StreamVault's
     * SyncManager reliably hit "Failed to connect to /127.0.0.1:8337" because the service
     * (and its listening socket) had already been reclaimed after the discovery bind that
     * handed out the provider URL completed. Mirrors jopsis's reference plugin
     * (StreamVaultAdaptiveBridgePluginService.keepAliveIfEnabled()) — a proven pattern on
     * this same StreamVault host contract.
     */
    private void keepAlive() {
        try {
            startService(new Intent(this, FastChannelsPluginService.class));
        } catch (Throwable e) {
            Log.w(TAG, "keepAlive startService failed", e);
        }
    }

    private void promoteToForeground() {
        try {
            createNotificationChannel();
            Notification notification = buildNotification();
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                startForeground(NOTIFICATION_ID, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC);
            } else {
                startForeground(NOTIFICATION_ID, notification);
            }
        } catch (Throwable e) {
            Log.w(TAG, "promoteToForeground failed", e);
        }
    }

    private Notification buildNotification() {
        Notification.Builder builder = Build.VERSION.SDK_INT >= Build.VERSION_CODES.O
                ? new Notification.Builder(this, NOTIFICATION_CHANNEL_ID)
                : new Notification.Builder(this);
        return builder
                .setContentTitle("FastChannels")
                .setContentText("Bridging live channels to StreamVault")
                .setSmallIcon(android.R.drawable.stat_sys_download_done)
                .setOngoing(true)
                .build();
    }

    private void createNotificationChannel() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) return;
        NotificationManager manager = (NotificationManager) getSystemService(NOTIFICATION_SERVICE);
        if (manager == null || manager.getNotificationChannel(NOTIFICATION_CHANNEL_ID) != null) return;
        NotificationChannel channel = new NotificationChannel(
                NOTIFICATION_CHANNEL_ID, "FastChannels sync", NotificationManager.IMPORTANCE_LOW);
        manager.createNotificationChannel(channel);
    }

    private boolean handleMessage(Message request) {
        Log.i(TAG, "handleMessage what=" + request.what);
        Bundle response = newResponse(request);
        try {
            switch (request.what) {
                case PluginContract.MSG_GET_MANIFEST:
                    response.putBoolean(PluginContract.KEY_SUCCESS, true);
                    response.putString(PluginContract.KEY_MANIFEST_JSON, PluginManifest.manifestJson().toString());
                    break;
                case PluginContract.MSG_SET_ENABLED:
                    response.putBoolean(PluginContract.KEY_SUCCESS, true);
                    break;
                case PluginContract.MSG_GET_STATUS:
                    handleStatus(response);
                    break;
                case PluginContract.MSG_GET_PROVIDER_URL:
                    handleProviderUrl(response);
                    break;
                case PluginContract.MSG_PREPARE_PLAYBACK:
                    handlePreparePlayback(request.getData(), response);
                    break;
                case PluginContract.MSG_REWRITE_CAST_URL:
                    response.putBoolean(PluginContract.KEY_SUCCESS, true);
                    response.putBoolean(PluginContract.KEY_HANDLED, false);
                    break;
                case PluginContract.MSG_GET_CONFIGURATION_SCHEMA:
                    response.putBoolean(PluginContract.KEY_SUCCESS, true);
                    response.putString(PluginContract.KEY_CONFIGURATION_SCHEMA_JSON, PluginManifest.configurationSchema().toString());
                    break;
                case PluginContract.MSG_GET_CONFIGURATION_VALUES:
                    response.putBoolean(PluginContract.KEY_SUCCESS, true);
                    response.putString(PluginContract.KEY_CONFIGURATION_VALUES_JSON,
                            new JSONObject().put("server_url", configStore.serverUrl()).toString());
                    break;
                case PluginContract.MSG_SET_CONFIGURATION_VALUES:
                    handleSetConfiguration(request.getData(), response);
                    break;
                case PluginContract.MSG_RUN_CONFIGURATION_ACTION:
                    handleRunAction(request.getData(), response);
                    break;
                default:
                    response.putBoolean(PluginContract.KEY_SUCCESS, false);
                    response.putString(PluginContract.KEY_MESSAGE, "unsupported request: " + request.what);
                    break;
            }
        } catch (Throwable error) {
            response.putBoolean(PluginContract.KEY_SUCCESS, false);
            response.putString(PluginContract.KEY_MESSAGE, error.getMessage() == null ? error.toString() : error.getMessage());
        }
        sendResponse(request, response);
        return true;
    }

    private void handleStatus(Bundle response) {
        boolean configured = !configStore.serverUrl().isEmpty();
        response.putBoolean(PluginContract.KEY_SUCCESS, true);
        response.putString(PluginContract.KEY_STATUS_LABEL, configured ? "Ready" : "Not configured");
        response.putString(PluginContract.KEY_MESSAGE,
                configured ? "FastChannels server: " + configStore.serverUrl() : "Set your FastChannels server URL in plugin settings.");
    }

    private void handleProviderUrl(Bundle response) {
        boolean configured = !configStore.serverUrl().isEmpty();
        response.putBoolean(PluginContract.KEY_SUCCESS, configured);
        response.putString(PluginContract.KEY_PROVIDER_NAME, "FastChannels");
        response.putString(PluginContract.KEY_URL, localServer.playlistUrl());
        if (!configured) {
            response.putString(PluginContract.KEY_MESSAGE, "FastChannels server URL is not configured.");
        }
    }

    private void handlePreparePlayback(Bundle request, Bundle response) throws Exception {
        String inputUrl = request.getString(PluginContract.KEY_INPUT_URL, "");
        Log.i(TAG, "prepare_playback input_url=" + inputUrl);
        if (!localServer.ownsUrl(inputUrl)) {
            Log.w(TAG, "prepare_playback: not our URL, declining");
            response.putBoolean(PluginContract.KEY_SUCCESS, true);
            response.putBoolean(PluginContract.KEY_HANDLED, false);
            return;
        }

        LocalPlaylistServer.ChannelEntry entry = localServer.lookup(inputUrl);
        Log.i(TAG, "prepare_playback lookup result=" + (entry == null ? "MISS" : ("HIT playUrl=" + entry.playUrl + " drm=" + entry.drm)));
        response.putBoolean(PluginContract.KEY_HANDLED, true);
        if (entry == null) {
            response.putBoolean(PluginContract.KEY_SUCCESS, false);
            response.putString(PluginContract.KEY_MESSAGE, "channel not found (playlist may need a refresh)");
            return;
        }

        String outputUrl = entry.playUrl;
        if (entry.drm && outputUrl.endsWith(".m3u8")) {
            // FastChannels' generic /play/<source>/<id>.m3u8 route 302-redirects to the real
            // manifest regardless of DRM status — that's fine for Kodi's inputstream.adaptive,
            // which is extension-agnostic and just follows the redirect. StreamVault appears to
            // pick its media source (HLS vs DASH) off the URL extension itself rather than our
            // stream_type hint: confirmed live 2026-08-24, ExoPlayer kept re-GETting the .m3u8
            // URL every 1-5s like an HLS live-playlist refresh instead of loading it once as a
            // DASH manifest. Every DRM source has its own dedicated .../dash.mpd proxy route
            // (see app/routes/play.py: amazon_prime_free, roku, cox, philo, sling, vidaa, pbs,
            // nbc_tve all follow this exact shape) — route DRM channels there instead.
            outputUrl = outputUrl.substring(0, outputUrl.length() - ".m3u8".length()) + "/dash.mpd";
        }
        Log.i(TAG, "prepare_playback output_url=" + outputUrl + " drm=" + entry.drm);

        response.putBoolean(PluginContract.KEY_SUCCESS, true);
        response.putString(PluginContract.KEY_OUTPUT_URL, outputUrl);
        response.putString(PluginContract.KEY_STREAM_TYPE, entry.drm ? "DASH" : "HLS");
        response.putString(PluginContract.KEY_HEADERS_JSON, "{}");
        response.putString(PluginContract.KEY_USER_AGENT, "");
        if (entry.drm) {
            JSONObject drm = new JSONObject()
                    .put("scheme", "widevine")
                    .put("licenseUrl", entry.licenseUrl)
                    .put("headers", new JSONObject());
            response.putString(PluginContract.KEY_DRM_JSON, drm.toString());
        }
    }

    private void handleSetConfiguration(Bundle request, Bundle response) throws Exception {
        String raw = request.getString(PluginContract.KEY_CONFIGURATION_VALUES_JSON, "{}");
        JSONObject values = new JSONObject(raw == null || raw.trim().isEmpty() ? "{}" : raw);
        configStore.setServerUrl(values.optString("server_url", ""));
        response.putBoolean(PluginContract.KEY_SUCCESS, true);
        response.putString(PluginContract.KEY_MESSAGE, "Settings saved.");
    }

    private void handleRunAction(Bundle request, Bundle response) {
        String actionId = request.getString(PluginContract.KEY_CONFIGURATION_ACTION_ID, "");
        if (!"test_connection".equals(actionId)) {
            response.putBoolean(PluginContract.KEY_SUCCESS, false);
            response.putString(PluginContract.KEY_MESSAGE, "unknown action: " + actionId);
            return;
        }
        String serverUrl = configStore.serverUrl();
        if (serverUrl.isEmpty()) {
            response.putBoolean(PluginContract.KEY_SUCCESS, false);
            response.putString(PluginContract.KEY_MESSAGE, "Set a FastChannels server URL first.");
            return;
        }
        try {
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) new java.net.URL(serverUrl + "/feeds/default/m3u").openConnection();
            conn.setConnectTimeout(6000);
            conn.setReadTimeout(6000);
            conn.setRequestMethod("HEAD");
            int status = conn.getResponseCode();
            conn.disconnect();
            boolean ok = status >= 200 && status < 400;
            response.putBoolean(PluginContract.KEY_SUCCESS, ok);
            response.putString(PluginContract.KEY_MESSAGE, ok ? "Connected." : "Server responded with HTTP " + status);
        } catch (Exception e) {
            response.putBoolean(PluginContract.KEY_SUCCESS, false);
            response.putString(PluginContract.KEY_MESSAGE, "Connection failed: " + e.getMessage());
        }
    }

    private Bundle newResponse(Message request) {
        Bundle response = new Bundle();
        Bundle data = request.getData();
        response.putInt(PluginContract.KEY_API_VERSION, PluginContract.API_VERSION);
        if (data != null) {
            response.putString(PluginContract.KEY_REQUEST_ID, data.getString(PluginContract.KEY_REQUEST_ID, ""));
        }
        return response;
    }

    private void sendResponse(Message request, Bundle response) {
        if (request.replyTo == null) return;
        Message reply = Message.obtain(null, request.what);
        reply.setData(response);
        try {
            request.replyTo.send(reply);
        } catch (RemoteException ignored) {
        }
    }
}
