FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    redis-server \
    ca-certificates \
    sqlite3 \
    libxml2-utils \
    xvfb \
    libgtk-3-0 \
    android-tools-adb \
    && rm -rf /var/lib/apt/lists/*

# Node.js 24 (NodeSource). yt-dlp's EJS engine needs a JS runtime to solve
# YouTube's n-signature challenge, and it requires Node >= 22 — Debian's stock
# node is older and rejected as "unsupported", which left the n-sig unsolved and
# made YouTube custom channels fail on many users' IPs.
RUN curl -fsSL https://deb.nodesource.com/setup_24.x | bash - \
    && apt-get update && apt-get install -y --no-install-recommends nodejs=24.13.0-1nodesource1 \
    && rm -rf /var/lib/apt/lists/*

# yt-dlp runs node with --permission (Node >= 23.5) for the EJS challenge, which
# then needs explicit filesystem-read + child-process grants. We can't grant those
# globally via NODE_OPTIONS because Playwright's node driver runs WITHOUT --permission
# and would crash (`--allow-* requires --permission`). So a shim named `node`
# (earlier on PATH) adds the grants only when --permission is present — i.e. only for
# yt-dlp — and passes every other node call (Playwright, version probes) straight through.
RUN printf '%s\n' \
    '#!/bin/sh' \
    'case " $* " in' \
    '  *" --permission "*|*" --experimental-permission "*)' \
    '    exec /usr/bin/node --no-warnings --allow-fs-read=* --allow-child-process "$@" ;;' \
    'esac' \
    'exec /usr/bin/node "$@"' \
    > /usr/local/bin/node \
    && chmod +x /usr/local/bin/node

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt
# Keep yt-dlp at GitHub master — YouTube extraction breaks on stale PyPI releases.
# This layer is cached by Docker's build cache, keyed on the RUN command's literal text.
# YTDLP_REFRESH must actually appear in that text (the `echo` below) or changing it does
# nothing — an ARG that's merely declared but never referenced in the RUN instruction does
# NOT bust its cache. Bump YTDLP_REFRESH (e.g. --build-arg YTDLP_REFRESH=$(date +%s)) to
# force a fresh pull; CI passes the commit SHA so every push gets current master.
ARG YTDLP_REFRESH=unset
RUN echo "yt-dlp refresh token: ${YTDLP_REFRESH}" \
    && pip install --force-reinstall "yt-dlp[default] @ https://github.com/yt-dlp/yt-dlp/archive/master.tar.gz"

RUN playwright install-deps chromium && playwright install chromium
# Real Google Chrome (not open-source Chromium) — DirecTV Stream's Akamai
# bot protection resets the connection for stock Chromium and its DRM check
# fails without a genuine Widevine CDM; real Chrome clears both (see
# app/scrapers/directv.py). If DirecTV's login ever falls back to patchright
# (a CDP-patched Playwright fork, currently unused — see the comment in that
# file), it shares this same browser registry, so no separate install step
# would be needed for it either.
RUN playwright install-deps chrome && playwright install chrome
# Camoufox (anti-detect Firefox) for the interactive Sling sign-in - fetches its
# own prebuilt browser binary; libgtk-3-0/xvfb above cover its runtime deps.
RUN python -m camoufox fetch

COPY . .

RUN chmod +x /app/entrypoint.sh

# Bundle the latest FastChannels Player release APK (app/fc_player/, a separate
# Android project — not built here) into the image so the settings-page "Install"
# flow (app/fc_player_bridge.py's install_app()/bundled_apk_path()) works with zero
# network dependency at install-button-click time — the only network dependency is
# here, at image-build time, same as everything else this Dockerfile already fetches.
# Deliberately NOT committed to git (see project memory on release signing) — this
# pulls the signed APK from its GitHub Release instead. Fails soft: no release
# published yet means no APK bundled and the install button reports that clearly,
# rather than breaking the whole image build. ARG cache-bust mirrors YTDLP_REFRESH
# above — bump it to force re-pulling whatever the latest release currently is,
# independent of any FastChannels code change landing in the same build.
ARG FC_PLAYER_APK_REFRESH=unset
RUN echo "fc-player APK refresh token: ${FC_PLAYER_APK_REFRESH}" \
    && (curl -fsSL -o /app/fc_player_release.apk.tmp \
        "https://github.com/kineticman/FastChannels/releases/latest/download/FastChannelsPlayer.apk" \
        && mv /app/fc_player_release.apk.tmp /app/fc_player_release.apk \
        && echo "Bundled FastChannels Player release APK." \
        || (rm -f /app/fc_player_release.apk.tmp \
            && echo "No FastChannels Player release available yet — install button will report unavailable."))

# Python's requests library defaults to its own bundled certifi CA store instead of the
# system one, and certifi doesn't always trust the same chains the OS does — confirmed
# live 2026-08-25: DirecTV's CDN cert (issued by "SSL Corporation / Cloudflare TLS
# Issuing ECC CA 3", chaining to an older SHA1-signed root) verified fine via curl
# (system store, ca-certificates package installed above) but failed with
# SSLCertVerificationError via requests. Pointing requests at the system bundle fixes it
# without disabling verification. Placed after the expensive install layers (apt/node/
# pip/playwright/chrome) so bumping this doesn't bust their Docker build cache.
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

EXPOSE 5523

ENTRYPOINT ["/app/entrypoint.sh"]
