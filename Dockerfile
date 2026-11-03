FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Set automatically by Buildx for each target platform. It must be declared in
# this build stage before a RUN instruction can read it.
ARG TARGETARCH

# No compiler: every requirement installs from a prebuilt wheel on amd64 and arm64
# (checked 2026-11-03). gcc + libpq-dev (nothing uses Postgres) cost ~145 MB. If a
# new dependency has no wheel the pip install fails loudly here rather than at runtime.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    redis-server \
    ca-certificates \
    sqlite3 \
    libxml2-utils \
    xvfb \
    libgtk-3-0 \
    libstdc++6 \
    android-tools-adb \
    && rm -rf /var/lib/apt/lists/*

# yt-dlp's EJS engine needs Node to solve YouTube's n-signature challenge. node-real
# is linked to the Node binary Playwright already ships (see after pip install below)
# rather than a second ~120 MB copy of Node.
#
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
    '    exec /usr/local/bin/node-real --no-warnings --allow-fs-read=* --allow-child-process "$@" ;;' \
    'esac' \
    'exec /usr/local/bin/node-real "$@"' \
    > /usr/local/bin/node \
    && chmod +x /usr/local/bin/node

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt
# Point node-real at Playwright's bundled Node. Playwright is unpinned, so its Node
# version follows the Playwright release: fail the build if it ever drops below the
# 23.5 yt-dlp needs (--permission), instead of YouTube silently losing formats.
RUN NODE_BIN="$(python -c 'import os, playwright; print(os.path.join(os.path.dirname(playwright.__file__), "driver", "node"))')" \
    && ln -sf "$NODE_BIN" /usr/local/bin/node-real \
    && node-real -e 'const [a, b] = process.versions.node.split(".").map(Number); if (a < 23 || (a === 23 && b < 5)) { console.error("Playwright Node " + process.version + " is older than the 23.5 yt-dlp needs"); process.exit(1); }' \
    && echo "node-real -> $NODE_BIN ($(node --version))"
# Keep yt-dlp at GitHub master — YouTube extraction breaks on stale PyPI releases.
# This layer is cached by Docker's build cache, keyed on the RUN command's literal text.
# YTDLP_REFRESH must actually appear in that text (the `echo` below) or changing it does
# nothing — an ARG that's merely declared but never referenced in the RUN instruction does
# NOT bust its cache. Bump YTDLP_REFRESH (e.g. --build-arg YTDLP_REFRESH=$(date +%s)) to
# force a fresh pull; CI passes the commit SHA so every push gets current master.
ARG YTDLP_REFRESH=unset
RUN echo "yt-dlp refresh token: ${YTDLP_REFRESH}" \
    && pip install --force-reinstall "yt-dlp[default] @ https://github.com/yt-dlp/yt-dlp/archive/master.tar.gz"

# Headless shell only (not the ~390 MB full Chromium): every p.chromium.launch() in
# app/ is headless=True, which Playwright serves from chromium-headless-shell. A
# headed Chromium launch would fail with "Executable doesn't exist" - install full
# chromium again if one is ever added.
RUN playwright install-deps chromium && playwright install --only-shell chromium
# Real Google Chrome (not open-source Chromium) is an amd64-only Playwright
# download. DirecTV Stream's Akamai protection needs it (see app/scrapers/directv.py,
# which always launches channel='chrome', headed on Xvfb). Playwright has no
# Chrome-for-Linux-arm64 build, so DirecTV browser sign-in is unavailable on arm64.
RUN if [ "$TARGETARCH" = "amd64" ]; then \
        playwright install-deps chrome && playwright install chrome; \
    else \
        echo "Skipping unsupported Playwright Chrome download on $TARGETARCH"; \
    fi
# Camoufox (anti-detect Firefox) for the TVE/Sling/Spectrum browser sign-ins -
# fetches its own prebuilt browser binary; libgtk-3-0/xvfb above cover its runtime deps.
# The bundle ships spoofing fonts for all three OSes, grouped by which OS reads them
# (fonts/groups.json: L, M, W, LM, LW, MW, LMW). Every Camoufox launch in this repo
# uses os='windows', which only reads LMW/LW/MW/W, so the macOS-only (M) and Linux-only
# (L) groups are dead weight: ~1 GB of 2.1 GB in 156.0.1-beta.36, which doubled the
# image when it landed. Camoufox builds its fontconfig from groups.json and skips
# missing group dirs. Older bundles used fonts/<os>/ instead, so trim those too.
# The trim also fixes a fingerprint leak: camoufox.cfg sets gfx.bundled-fonts.activate,
# which adds the whole fonts/ tree recursively, so with M present a "Windows" browser drew
# Arial from macOS's Arial file (canvas width 393 vs 388 with Windows' arial.ttf).
# If a launch ever spoofs macOS or Linux, remove the matching rm below.
RUN python -m camoufox fetch \
    && for d in /root/.cache/camoufox/browsers/*/*/fonts; do \
        rm -rf "$d/M" "$d/L" "$d/macos" "$d/linux"; \
    done \
    && du -sh /root/.cache/camoufox

COPY . .

RUN chmod +x /app/entrypoint.sh

# Bundle the latest FastChannels Player release APK (app/fc_player/, a separate
# Android project — not built here) into the image so the settings-page "Install"
# flow (app/fc_player_bridge.py's install_app()/bundled_apk_path()) works with zero
# network dependency at install-button-click time — the only network dependency is
# here, at image-build time, same as everything else this Dockerfile already fetches.
# Deliberately NOT committed to git (see project memory on release signing) — this
# pulls the signed APK from its GitHub Release instead. A missing asset or temporary
# fetch failure leaves the image usable and makes the install button report that no
# APK was bundled, rather than breaking the whole image build. ARG cache-bust mirrors
# YTDLP_REFRESH above — bump it to force re-pulling whatever the latest release
# currently is, independent of any FastChannels code change landing in the same build.
ARG FC_PLAYER_APK_REFRESH=unset
RUN echo "fc-player APK refresh token: ${FC_PLAYER_APK_REFRESH}" \
    && (curl -fsSL -o /app/fc_player_release.apk.tmp \
        "https://github.com/kineticman/FastChannels/releases/latest/download/FastChannelsPlayer.apk" \
        && mv /app/fc_player_release.apk.tmp /app/fc_player_release.apk \
        && echo "Bundled FastChannels Player release APK." \
        || (rm -f /app/fc_player_release.apk.tmp \
            && echo "FastChannels Player APK was not available — install button will report unavailable."))

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
