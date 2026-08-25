package com.fastchannels.streamvault.bridge;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Runtime copy of the plugin manifest (returned from MSG_GET_MANIFEST) and the
 * configuration.schema this plugin exposes. Keep manifestJson() in sync with the
 * android:value="..." meta-data string in AndroidManifest.xml — that static copy is
 * what StreamVault reads at discovery time, before it ever binds the service.
 */
final class PluginManifest {
    private PluginManifest() {
    }

    static JSONObject manifestJson() throws Exception {
        return new JSONObject()
                .put("schemaVersion", 1)
                .put("id", "com.fastchannels.streamvault.bridge")
                .put("name", "FastChannels")
                .put("versionName", "1.0.0")
                .put("versionCode", 1)
                .put("description", "FastChannels live channels and DRM playback via your FastChannels server.")
                .put("providerName", "FastChannels")
                .put("playbackUrlSchemes", new JSONArray().put("http"))
                .put("playbackUrlHosts", new JSONArray().put("127.0.0.1"))
                .put("playbackPriority", 50)
                .put("configurationMode", "host.schema")
                .put("configurationActivityAction", "")
                .put("capabilities", new JSONArray()
                        .put("provider.m3u")
                        .put("playback.prepare")
                        .put("configuration.schema"));
    }

    static JSONObject configurationSchema() throws Exception {
        JSONObject serverUrlField = new JSONObject()
                .put("key", "server_url")
                .put("type", "url")
                .put("label", "FastChannels Server URL")
                .put("placeholder", "http://192.168.1.x:5523")
                .put("required", true);
        JSONObject section = new JSONObject()
                .put("id", "connection")
                .put("title", "Connection")
                .put("fields", new JSONArray().put(serverUrlField));
        JSONObject testAction = new JSONObject()
                .put("id", "test_connection")
                .put("label", "Test Connection")
                .put("description", "Checks that the FastChannels server is reachable.")
                .put("refreshAfterRun", false);
        return new JSONObject()
                .put("schemaVersion", 1)
                .put("title", "FastChannels")
                .put("description", "Point this plugin at your FastChannels server.")
                .put("sections", new JSONArray().put(section))
                .put("actions", new JSONArray().put(testAction));
    }
}
