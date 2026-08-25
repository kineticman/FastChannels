package com.fastchannels.streamvault.bridge;

import android.content.Context;
import android.content.SharedPreferences;

/** Persists the one piece of config this plugin needs: the FastChannels server base URL. */
final class ConfigStore {
    private static final String PREFS_NAME = "fastchannels_bridge";
    private static final String KEY_SERVER_URL = "server_url";

    // TODO(dev-only): remove before any public/release build — defaults the plugin to
    // the dev box's FastChannels-test server so config entry can be skipped while
    // iterating. A real install must never ship with a hardcoded LAN address.
    private static final String DEV_DEFAULT_SERVER_URL = "http://192.168.86.72:5525";

    private final SharedPreferences prefs;

    ConfigStore(Context context) {
        prefs = context.getApplicationContext().getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE);
    }

    String serverUrl() {
        String value = prefs.getString(KEY_SERVER_URL, "");
        value = value == null ? "" : value.trim();
        return value.isEmpty() ? DEV_DEFAULT_SERVER_URL : value;
    }

    void setServerUrl(String url) {
        String trimmed = url == null ? "" : url.trim();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        prefs.edit().putString(KEY_SERVER_URL, trimmed).apply();
    }
}
