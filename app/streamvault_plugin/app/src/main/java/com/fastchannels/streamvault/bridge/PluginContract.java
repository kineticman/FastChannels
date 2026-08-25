package com.fastchannels.streamvault.bridge;

/**
 * Message codes and Bundle keys per StreamVault's plugin IPC contract — verified against
 * StreamVault-IPTV-Plugin-Adaptive-Bridge's PluginContract.java / *PluginService.java
 * (github.com/jopsis/StreamVault-IPTV-Plugin-Adaptive-Bridge), not guessed.
 */
final class PluginContract {
    static final int API_VERSION = 1;

    static final int MSG_GET_MANIFEST = 1;
    static final int MSG_SET_ENABLED = 2;
    static final int MSG_GET_STATUS = 3;
    static final int MSG_GET_PROVIDER_URL = 4;
    static final int MSG_PREPARE_PLAYBACK = 5;
    static final int MSG_REWRITE_CAST_URL = 6;
    static final int MSG_GET_CONFIGURATION_SCHEMA = 7;
    static final int MSG_GET_CONFIGURATION_VALUES = 8;
    static final int MSG_SET_CONFIGURATION_VALUES = 9;
    static final int MSG_RUN_CONFIGURATION_ACTION = 10;

    static final String KEY_API_VERSION = "api_version";
    static final String KEY_REQUEST_ID = "request_id";
    static final String KEY_SUCCESS = "success";
    static final String KEY_HANDLED = "handled";
    static final String KEY_ENABLED = "enabled";
    static final String KEY_MESSAGE = "message";
    static final String KEY_MANIFEST_JSON = "manifest_json";
    static final String KEY_STATUS_LABEL = "status_label";
    static final String KEY_URL = "url";
    static final String KEY_PROVIDER_NAME = "provider_name";
    static final String KEY_INPUT_URL = "input_url";
    static final String KEY_OUTPUT_URL = "output_url";
    static final String KEY_CONFIGURATION_SCHEMA_JSON = "configuration_schema_json";
    static final String KEY_CONFIGURATION_VALUES_JSON = "configuration_values_json";
    static final String KEY_CONFIGURATION_ACTION_ID = "configuration_action_id";

    static final String KEY_STREAM_TYPE = "stream_type";
    static final String KEY_HEADERS_JSON = "headers_json";
    static final String KEY_USER_AGENT = "user_agent";
    static final String KEY_DRM_JSON = "drm_json";

    private PluginContract() {
    }
}
