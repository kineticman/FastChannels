package com.fastchannels.streamvault.bridge;

import android.util.Log;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Plugin-owned local HTTP server, bound to all interfaces (harmless — read-only playlist data,
 * same no-auth trust model as the rest of FastChannels — and convenient for LAN debugging).
 *
 * Serves the rewritten playlist StreamVault fetches for provider.m3u (GET /playlist.m3u,
 * proxying and translating FastChannels' own /m3u/streamvault), and a /play endpoint whose URLs
 * exist only so provider.m3u entries carry a scheme+host (http://127.0.0.1:PORT) this plugin
 * declared ownership of via playbackUrlSchemes/playbackUrlHosts in the manifest — that's what
 * makes StreamVault route those channels' MSG_PREPARE_PLAYBACK calls to us at all (see
 * PluginPlaybackRouting.kt in the StreamVault host: it matches strictly on the URL already
 * sitting in the playlist, not on the playback.prepare *response*). The response's output_url
 * is free to point straight at FastChannels — ExoPlayer fetches it directly, so /play is never
 * actually GET-ed by StreamVault in the normal flow; it 302s to the real FastChannels URL
 * defensively in case something does hit it directly.
 *
 * The remote-play trigger (FastChannels' analog of kodi_bridge.trigger_channel()) does NOT go
 * through this server — see PlaybackActivity's docstring for why (an in-process
 * context.startActivity() from this server's own thread is blocked by Android's
 * background-activity-start restriction, and Fire OS additionally blocks the standard
 * full-screen-intent-notification workaround via its own notification allowlist; both
 * confirmed live 2026-08-24). FastChannels' backend instead launches PlaybackActivity directly
 * via `adb shell am start`, exactly like kodi_bridge.py already does for Kodi.
 */
final class LocalPlaylistServer {
    private static final String TAG = "FCBridge.LocalServer";
    static final int PORT = 8337;
    private static final String KODIPROP_PREFIX = "#KODIPROP:";
    private static final String LICENSE_KEY_PROP = "inputstream.adaptive.license_key";

    static final class ChannelEntry {
        final String playUrl;
        final boolean drm;
        final String licenseUrl;

        ChannelEntry(String playUrl, boolean drm, String licenseUrl) {
            this.playUrl = playUrl;
            this.drm = drm;
            this.licenseUrl = licenseUrl;
        }

        /**
         * The URL StreamVault's playback.prepare response should actually hand ExoPlayer.
         * FastChannels' generic /play/&lt;source&gt;/&lt;id&gt;.m3u8 route 302-redirects to
         * the real manifest regardless of DRM status — fine for Kodi's inputstream.adaptive
         * (extension-agnostic, just follows the redirect), but StreamVault picks HLS vs DASH
         * off the URL extension itself. Every DRM source has its own dedicated .../dash.mpd
         * proxy route (see app/routes/play.py: amazon_prime_free, roku, cox, philo, sling,
         * vidaa, pbs, nbc_tve) — route DRM channels there instead.
         */
        String resolvedPlayUrl() {
            if (drm && playUrl.endsWith(".m3u8")) {
                return playUrl.substring(0, playUrl.length() - ".m3u8".length()) + "/dash.mpd";
            }
            return playUrl;
        }
    }

    private final ConfigStore configStore;
    // Keyed by FastChannels' own stable channel-id (e.g. "roku.2fc3..."), NOT a per-fetch
    // auto-incrementing counter. StreamVault persists the M3U URLs it imports into its own
    // catalog and reuses them for playback.prepare at any later time, including well after
    // this cache has been rebuilt by a subsequent /playlist.m3u fetch — an incrementing id
    // would silently point at a different channel (or nothing) once ordering/count shifted
    // between fetches. The channel-id is deterministic from FastChannels' own DB, so the
    // same channel always maps to the same local URL regardless of how many times the
    // playlist gets rebuilt. Confirmed live 2026-08-24: an old sequential id produced
    // "channel not found (playlist may need a refresh)" after just one extra fetch.
    private final Map<String, ChannelEntry> cache = new ConcurrentHashMap<>();
    private volatile boolean running;
    private ServerSocket serverSocket;
    private Thread acceptThread;

    LocalPlaylistServer(ConfigStore configStore) {
        this.configStore = configStore;
    }

    String baseUrl() {
        return "http://127.0.0.1:" + PORT;
    }

    String playlistUrl() {
        return baseUrl() + "/playlist.m3u";
    }

    boolean ownsUrl(String url) {
        return url != null && url.startsWith(baseUrl() + "/play");
    }

    /** Looks up a previously cached channel by its local /play?u=&lt;id&gt; URL. */
    ChannelEntry lookup(String localPlayUrl) {
        String id = extractId(localPlayUrl);
        return id == null ? null : cache.get(id);
    }

    private static String extractId(String localPlayUrl) {
        if (localPlayUrl == null) return null;
        int q = localPlayUrl.indexOf("u=");
        if (q < 0) return null;
        String rest = localPlayUrl.substring(q + 2);
        int amp = rest.indexOf('&');
        String idStr = amp >= 0 ? rest.substring(0, amp) : rest;
        try {
            return java.net.URLDecoder.decode(idStr, "UTF-8");
        } catch (Exception e) {
            return idStr;
        }
    }

    synchronized void start() {
        Log.i(TAG, "start() called, running=" + running);
        if (running) return;
        running = true;
        acceptThread = new Thread(this::acceptLoop, "fc-bridge-local-server");
        acceptThread.setDaemon(true);
        acceptThread.start();
        Log.i(TAG, "accept thread started");
    }

    synchronized void stop() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {
        }
    }

    private void acceptLoop() {
        try {
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            // All interfaces, not just loopback — see the class docstring. StreamVault only
            // ever needs 127.0.0.1, which this also satisfies.
            serverSocket.bind(new InetSocketAddress(PORT));
            Log.i(TAG, "bound and listening on " + serverSocket.getLocalSocketAddress());
            while (running) {
                Socket socket;
                try {
                    socket = serverSocket.accept();
                    Log.i(TAG, "accepted connection from " + socket.getRemoteSocketAddress());
                } catch (IOException e) {
                    if (!running) return;
                    Log.w(TAG, "accept() failed", e);
                    continue;
                }
                Thread client = new Thread(() -> handleClient(socket), "fc-bridge-client");
                client.setDaemon(true);
                client.start();
            }
        } catch (Throwable e) {
            Log.e(TAG, "acceptLoop bind/listen failed", e);
            running = false;
        }
    }

    private void handleClient(Socket socket) {
        try (Socket s = socket;
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
             OutputStream out = s.getOutputStream()) {
            String requestLine = reader.readLine();
            if (requestLine == null || requestLine.isEmpty()) return;
            String[] parts = requestLine.split(" ");
            String path = parts.length > 1 ? parts[1] : "/";
            String line;
            while ((line = reader.readLine()) != null && !line.isEmpty()) {
                // drain headers, we don't need them
            }
            if (path.startsWith("/playlist.m3u")) {
                handlePlaylist(out);
            } else if (path.startsWith("/play")) {
                handlePlay(path, out);
            } else {
                writeResponse(out, 404, "Not Found", "text/plain", "not found".getBytes(StandardCharsets.UTF_8), null);
            }
        } catch (Exception e) {
            Log.w(TAG, "handleClient failed", e);
        }
    }

    private void handlePlaylist(OutputStream out) throws IOException {
        String serverUrl = configStore.serverUrl();
        if (serverUrl.isEmpty()) {
            writeResponse(out, 503, "Service Unavailable", "text/plain",
                    "FastChannels server URL is not configured".getBytes(StandardCharsets.UTF_8), null);
            return;
        }
        try {
            String playlist = buildPlaylist(serverUrl);
            Log.i(TAG, "playlist built, " + cache.size() + " channel(s)");
            writeResponse(out, 200, "OK", "audio/x-mpegurl", playlist.getBytes(StandardCharsets.UTF_8), null);
        } catch (Exception e) {
            Log.e(TAG, "failed to fetch/build playlist from " + serverUrl, e);
            writeResponse(out, 502, "Bad Gateway", "text/plain",
                    ("failed to fetch FastChannels playlist: " + e).getBytes(StandardCharsets.UTF_8), null);
        }
    }

    private void handlePlay(String path, OutputStream out) throws IOException {
        ChannelEntry entry = lookup(path);
        if (entry == null) {
            writeResponse(out, 404, "Not Found", "text/plain", "channel not found".getBytes(StandardCharsets.UTF_8), null);
            return;
        }
        writeResponse(out, 302, "Found", "text/plain", new byte[0], "Location: " + entry.playUrl);
    }

    private static final java.util.regex.Pattern CHANNEL_ID_ATTR =
            java.util.regex.Pattern.compile("channel-id=\"([^\"]*)\"");

    /**
     * Fetches FastChannels' own KODIPROP-tagged M3U (app/generators/m3u.py generate_m3u())
     * and rewrites it into a plain playlist StreamVault can parse directly: strips
     * #KODIPROP lines, replaces each play URL with a local one, and remembers the
     * real play URL + DRM license URL (parsed out of the KODIPROP license_key line,
     * which FastChannels always emits as "<url>||R{SSM}|" — see app/scrapers/base.py) in
     * an in-memory cache keyed by FastChannels' own stable channel-id attribute (see the
     * cache field's docstring for why a per-fetch counter isn't safe here).
     */
    private String buildPlaylist(String serverUrl) throws Exception {
        // /m3u/streamvault (app/generators/m3u.py generate_streamvault_m3u) rather than the
        // default feed — the default feed's base 'active' selection excludes every channel
        // flagged requires_drm_bridge, which for an all-DRM source like amazon_prime_free
        // means NONE of its channels show up there at all. This feed unions the DRM-bridge
        // channels back in (KODI_BRIDGE_TRUSTED_SOURCES only) same as the Kodi-bridge feed.
        String upstream = fetchText(serverUrl + "/m3u/streamvault");
        cache.clear();
        StringBuilder rewritten = new StringBuilder("#EXTM3U\n");
        List<String> pendingKodiProps = new ArrayList<>();
        String pendingExtinf = null;

        for (String rawLine : upstream.split("\r?\n")) {
            String line = rawLine.trim();
            if (line.isEmpty() || line.equals("#EXTM3U")) continue;
            if (line.startsWith(KODIPROP_PREFIX)) {
                pendingKodiProps.add(line.substring(KODIPROP_PREFIX.length()));
                continue;
            }
            if (line.startsWith("#EXTINF")) {
                pendingExtinf = line;
                continue;
            }
            if (line.startsWith("#")) continue;

            // Non-comment, non-empty line after an EXTINF is the channel's play URL.
            String originalPlayUrl = line;
            String licenseUrl = null;
            for (String prop : pendingKodiProps) {
                int eq = prop.indexOf('=');
                if (eq < 0) continue;
                String key = prop.substring(0, eq);
                if (!key.equals(LICENSE_KEY_PROP)) continue;
                String value = prop.substring(eq + 1);
                int pipe = value.indexOf('|');
                licenseUrl = pipe >= 0 ? value.substring(0, pipe) : value;
            }
            boolean drm = licenseUrl != null && !licenseUrl.isEmpty();

            String id = null;
            if (pendingExtinf != null) {
                java.util.regex.Matcher m = CHANNEL_ID_ATTR.matcher(pendingExtinf);
                if (m.find()) id = m.group(1);
            }
            if (id == null || id.isEmpty()) {
                // No channel-id attribute (shouldn't happen for FastChannels' own M3U output,
                // but fall back to something at least stable across fetches rather than an
                // incrementing counter): the real play URL itself is already a stable,
                // deterministic identifier for this channel.
                id = String.valueOf(originalPlayUrl.hashCode());
            }
            cache.put(id, new ChannelEntry(originalPlayUrl, drm, licenseUrl));

            if (pendingExtinf != null) {
                rewritten.append(pendingExtinf).append('\n');
            }
            rewritten.append(baseUrl()).append("/play?u=")
                    .append(java.net.URLEncoder.encode(id, "UTF-8")).append('\n');

            pendingKodiProps.clear();
            pendingExtinf = null;
        }
        return rewritten.toString();
    }

    private String fetchText(String urlStr) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setConnectTimeout(8000);
        conn.setReadTimeout(20000);
        conn.setRequestMethod("GET");
        try (InputStream in = conn.getInputStream()) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            byte[] chunk = new byte[8192];
            int read;
            while ((read = in.read(chunk)) != -1) {
                buffer.write(chunk, 0, read);
            }
            return buffer.toString("UTF-8");
        } finally {
            conn.disconnect();
        }
    }

    private void writeResponse(OutputStream out, int status, String statusText, String contentType,
                                byte[] body, String extraHeader) throws IOException {
        StringBuilder headers = new StringBuilder();
        headers.append("HTTP/1.1 ").append(status).append(' ').append(statusText).append("\r\n");
        headers.append("Content-Type: ").append(contentType).append("\r\n");
        headers.append("Content-Length: ").append(body.length).append("\r\n");
        headers.append("Connection: close\r\n");
        if (extraHeader != null) {
            headers.append(extraHeader).append("\r\n");
        }
        headers.append("\r\n");
        out.write(headers.toString().getBytes(StandardCharsets.UTF_8));
        out.write(body);
        out.flush();
    }
}
