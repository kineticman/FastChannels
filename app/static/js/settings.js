const settingsBootstrap = window.FC_SETTINGS_BOOTSTRAP || {};
const settingsNeedsConfigState = settingsBootstrap.settingsNeedsConfigState || {};

function scrollToSettingCard(anchorId) {
  const el = document.getElementById(anchorId);
  if (!el) return;
  el.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function settingsSectionLinks(container) {
  return Array.from(container.querySelectorAll('a[href^="#settings-section-"]'));
}

function syncSettingsSectionNavActive(navContainers) {
  const sourceNav = navContainers[0];
  if (!sourceNav) return;
  const sourceLinks = settingsSectionLinks(sourceNav);
  const sections = sourceLinks
    .map((link) => document.getElementById(link.getAttribute('href').slice(1)))
    .filter(Boolean);
  if (!sourceLinks.length || !sections.length) return;

  const floatingNav = navContainers.find((nav) => nav.classList.contains('settings-section-nav-floating'));
  const navBottom = (floatingNav && floatingNav.classList.contains('is-visible'))
    ? floatingNav.getBoundingClientRect().bottom
    : 0;
  const activationLine = navBottom + 96;
  let activeId = sections[0].id;
  for (const section of sections) {
    if (section.getBoundingClientRect().top <= activationLine) activeId = section.id;
  }

  navContainers.forEach((nav) => {
    settingsSectionLinks(nav).forEach((link) => {
      const isActive = link.getAttribute('href') === `#${activeId}`;
      link.classList.toggle('active', isActive);
      if (isActive) {
        link.setAttribute('aria-current', 'true');
        const left = link.offsetLeft;
        const right = left + link.offsetWidth;
        if (left < nav.scrollLeft) nav.scrollLeft = left;
        if (right > nav.scrollLeft + nav.clientWidth) nav.scrollLeft = right - nav.clientWidth;
      } else {
        link.removeAttribute('aria-current');
      }
    });
  });
}

function initSettingsSectionNav() {
  const sourceNav = document.querySelector('.settings-section-nav');
  if (!sourceNav) return;

  const floatingNav = document.createElement('div');
  floatingNav.className = 'settings-section-nav-floating';
  floatingNav.setAttribute('role', 'navigation');
  floatingNav.setAttribute('aria-label', 'Settings sections');
  floatingNav.innerHTML = sourceNav.innerHTML;
  document.body.appendChild(floatingNav);

  const adminNav = document.querySelector('body > nav');
  const navContainers = [sourceNav, floatingNav];
  const setActiveByHash = (hash) => {
    if (!hash || !hash.startsWith('#settings-section-')) return;
    navContainers.forEach((nav) => {
      settingsSectionLinks(nav).forEach((link) => {
        const isActive = link.getAttribute('href') === hash;
        link.classList.toggle('active', isActive);
        if (isActive) link.setAttribute('aria-current', 'true');
        else link.removeAttribute('aria-current');
      });
    });
  };
  const adminNavOffset = () => {
    if (!adminNav || adminNav === sourceNav) return 0;
    return Math.max(0, Math.round(adminNav.getBoundingClientRect().bottom));
  };
  const update = () => {
    const topOffset = adminNavOffset();
    document.documentElement.style.setProperty('--settings-section-nav-top', `${topOffset}px`);
    const sourceRect = sourceNav.getBoundingClientRect();
    floatingNav.classList.toggle('is-visible', sourceRect.bottom <= topOffset);
    syncSettingsSectionNavActive(navContainers);
  };

  let pending = false;
  const requestUpdate = () => {
    if (pending) return;
    pending = true;
    window.requestAnimationFrame(() => {
      pending = false;
      update();
    });
  };

  navContainers.forEach((nav) => {
    settingsSectionLinks(nav).forEach((link) => {
      link.addEventListener('click', () => setActiveByHash(link.getAttribute('href')));
    });
  });

  update();
  window.addEventListener('scroll', requestUpdate, { passive: true });
  window.addEventListener('resize', requestUpdate);
  window.addEventListener('hashchange', () => {
    setActiveByHash(window.location.hash);
    requestUpdate();
  });
}

function refreshSettingsNeedsConfigBanner() {
  const banner = document.getElementById('settings-needs-config-banner');
  const list = document.getElementById('settings-needs-config-list');
  if (!banner || !list) return;

  Object.entries(settingsNeedsConfigState).forEach(([key, needsConfig]) => {
    const item = document.getElementById(`settings-needs-config-item-${key}`);
    if (!item) return;
    if (needsConfig) {
      item.classList.remove('item-configured');
      const note = item.querySelector('.config-note');
      if (note) note.textContent = ' — click to configure';
    } else {
      item.classList.add('item-configured');
      const note = item.querySelector('.config-note');
      if (note) note.textContent = ' — configured';
    }
  });

  const remaining = Object.values(settingsNeedsConfigState).filter(Boolean).length;
  const strong = banner.querySelector('strong');
  if (remaining === 0) {
    if (strong) strong.textContent = '✓ All settings configured';
    banner.classList.add('all-configured');
    setTimeout(() => banner.remove(), 3000);
    return;
  }

  if (strong) {
    strong.textContent = `⚠ ${remaining} setting${remaining === 1 ? '' : 's'} should be configured on new installs`;
  }
}

async function saveSettings(body, statusId) {
  const status = document.getElementById(statusId);
  status.className = 'save-status';
  status.textContent = 'Saving…';
  try {
    const r = await fetch('/api/settings', {
      method:  'POST',
      headers: {'Content-Type': 'application/json'},
      body:    JSON.stringify(body),
    });
    let payload = null;
    try { payload = await r.json(); } catch (_) {}
    if (!r.ok) throw new Error(payload?.error || `HTTP ${r.status}`);
    status.className = 'save-status ok';
    status.textContent = '✓ Saved';
    setTimeout(() => { status.textContent = ''; }, 2500);
    return true;
  } catch (e) {
    status.className = 'save-status error';
    status.textContent = '✕ ' + e.message;
    return false;
  }
}

function checkPublicBaseUrlPortWarning() {
  const val = document.getElementById('public-base-url').value.trim();
  const warning = document.getElementById('no-port-warning');
  if (!warning) return;
  if (!val) { warning.style.display = 'none'; return; }
  const s = val.includes('://') ? val : 'http://' + val;
  let hasPort = false;
  try { hasPort = !!new URL(s).port; } catch (_) { hasPort = true; }
  warning.style.display = hasPort ? 'none' : '';
}

function isPlaceholderPublicBaseUrl(value) {
  const raw = value.trim();
  if (!raw) return false;
  try {
    const parsed = new URL(raw.includes("://") ? raw : "http://" + raw);
    let host = parsed.hostname.toLowerCase();
    if (host.endsWith(".")) host = host.slice(0, -1);
    return ["example.com", "myserver.example.com", "your-server", "your-server-ip", "192.168.1.x"].includes(host)
      || host.endsWith(".example.com");
  } catch (_) {
    return false;
  }
}

async function savePublicBaseUrlSettings() {
  const publicBaseUrl = document.getElementById('public-base-url').value.trim();
  const ok = await saveSettings({
    public_base_url: publicBaseUrl || null,
  }, 'public-base-url-status');
  if (ok) {
    checkPublicBaseUrlPortWarning();
    settingsNeedsConfigState.public_base_url = !publicBaseUrl || isPlaceholderPublicBaseUrl(publicBaseUrl);
    refreshSettingsNeedsConfigBanner();
  }
}

async function savePrismcastSettings() {
  const url   = document.getElementById('prismcast-url').value.trim();
  const inner = document.getElementById('prismcast-inner-url').value.trim();
  const maxHeight = parseInt(document.getElementById('prismcast-max-height').value || '0', 10);
  await saveSettings({
    prismcast_url:       url || null,
    prismcast_inner_url: inner || null,
    prismcast_max_height: Number.isFinite(maxHeight) ? maxHeight : 0,
  }, 'prismcast-status');
}


const tveProviders = settingsBootstrap.tveProviders || [];
const tveProviderMap = Object.fromEntries(tveProviders.map((p) => [p.id, p]));
const tvePasswordPlaceholder = settingsBootstrap.tvePasswordPlaceholder || 'TV provider password';

function selectedTveProvider() {
  const select = document.getElementById('tve-provider');
  const providerId = select?.value || 'Cox';
  return tveProviderMap[providerId] || tveProviderMap.Cox || tveProviders[0] || {id: 'Cox', name: 'Cox'};
}

function filterTveProviders() {
  const query = (document.getElementById('tve-provider-search')?.value || '').trim().toLowerCase();
  const select = document.getElementById('tve-provider');
  if (!select) return;
  for (const option of select.options) {
    option.hidden = query && !option.textContent.toLowerCase().includes(query) && !option.value.toLowerCase().includes(query);
  }
}

function updateTveProviderFields() {
  const select = document.getElementById('tve-provider');
  const provider = selectedTveProvider();
  const username = document.getElementById('tve-cox-username');
  const password = document.getElementById('tve-cox-password');
  const adobeMso = document.getElementById('tve-cox-adobe-mso');
  const ytdlpMso = document.getElementById('tve-cox-ytdlp-mso');
  const hint = document.getElementById('tve-provider-hint');
  const providerChanged = select && select.dataset.previousProvider && select.dataset.previousProvider !== provider.id;
  if (username) username.placeholder = `${provider.name} username`;
  if (password) password.placeholder = tvePasswordPlaceholder;
  if (adobeMso && (providerChanged || !adobeMso.value.trim())) adobeMso.value = provider.id;
  if (ytdlpMso && (providerChanged || !ytdlpMso.value.trim())) ytdlpMso.value = provider.id;
  if (hint) {
    const backend = document.getElementById('tve-cox-auth-backend')?.value || 'native';
    const base = provider.id === 'Cox'
      ? 'Used by TVE sources such as A+E Networks TVE.'
      : `${provider.name} is available through yt-dlp Adobe Pass. Native A+E testing currently supports Cox only.`;
    hint.textContent = backend === 'yt_dlp'
      ? `${base} Advanced engine set to yt-dlp for future compatible TVE sources.`
      : base;
  }
  if (select) select.dataset.previousProvider = provider.id;
}

async function saveTveCoxSettings() {
  const status = document.getElementById('tve-cox-status');
  const body = {
    provider_id: document.getElementById('tve-provider').value,
    is_enabled: document.getElementById('tve-cox-enabled').checked,
    username: document.getElementById('tve-cox-username').value.trim(),
    password: document.getElementById('tve-cox-password').value,
    auth_backend: document.getElementById('tve-cox-auth-backend').value,
    adobe_mso_id: document.getElementById('tve-cox-adobe-mso').value.trim() || selectedTveProvider().id,
    yt_dlp_mso_id: document.getElementById('tve-cox-ytdlp-mso').value.trim() || document.getElementById('tve-cox-adobe-mso').value.trim() || selectedTveProvider().id,
    software_statement: document.getElementById('tve-cox-software-statement').value.trim(),
  };
  status.className = 'save-status';
  status.textContent = 'Saving…';
  try {
    const r = await fetch('/api/settings/tve/cox', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    const data = await r.json();
    if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);
    document.getElementById('tve-cox-password').value = '';
    document.getElementById('tve-cox-password').placeholder = data.password_configured ? 'Password saved - leave blank to keep' : 'TV provider password';
    status.className = 'save-status ok';
    status.textContent = '✓ Saved';
    setTimeout(() => { status.textContent = ''; }, 2500);
    return true;
  } catch (e) {
    status.className = 'save-status error';
    status.textContent = '✕ ' + e.message;
    return false;
  }
}

async function testTveCox() {
  const status = document.getElementById('tve-cox-status');
  const btn = document.getElementById('tve-cox-test-btn');
  const last = document.getElementById('tve-cox-last-status');
  const saved = await saveTveCoxSettings();
  if (!saved) return;
  status.className = 'save-status';
  status.textContent = 'Testing…';
  btn.disabled = true;
  try {
    const r = await fetch('/api/settings/tve/cox/test', { method: 'POST' });
    const data = await r.json();
    if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);
    status.className = 'save-status ok';
    status.textContent = '✓ Authorized';
    if (last) last.textContent = data.account?.last_auth_message || 'Cox authorized History via Adobe Pass.';
  } catch (e) {
    status.className = 'save-status error';
    status.textContent = '✕ Auth failed';
    if (last) last.textContent = e.message;
  } finally {
    btn.disabled = false;
  }
}

let _lastPrismcastDiagnostics = '';

function formatPrismcastDiagnostics(data) {
  const d = data.diagnostics || {};
  const settings = d.settings || {};
  const topology = d.topology || {};
  const resources = d.resources || {};
  const browserContext = d.browser_context || {};
  const health = d.health || {};
  const attempts = d.attempts || [];
  const candidates = d.candidates || [];
  const healthSnapshots = d.health_snapshots || [];
  const apiProbes = d.api_probes || [];
  const portProbes = d.port_probes || [];
  const streamText = h => h && h.streams ? `${h.streams.active}/${h.streams.limit}` : '(unknown)';
  const snapshotLine = snap => {
    const body = snap.body || {};
    return `  ${snap.phase || '(unknown)'}: HTTP ${snap.status_code == null ? '(none)' : snap.status_code}, ${snap.latency_ms == null ? '?' : snap.latency_ms}ms, status ${body.status || '(unknown)'}, streams ${streamText(body)}${snap.error ? `, error ${snap.error}` : ''}`;
  };
  const lines = [
    'PrismCast setup diagnostics',
    `Overall: ${data.overall_label || (data.overall || 'unknown').toUpperCase()}`,
    '',
    'Settings:',
    `  Public Base URL: ${settings.public_base_url || '(blank)'}`,
    `  PrismCast Server URL: ${settings.prismcast_url || '(blank)'}`,
    `  Watch-page URL base: ${settings.watch_page_url_base || '(blank)'}`,
    `  Channels DVR URL: ${settings.channels_dvr_url || '(blank)'}`,
    `  Bridge DRM enabled: ${settings.drm_bridge_enabled}`,
    `  PrismCast max height: ${settings.prismcast_max_height || 0}`,
    `  Diagnostic runtime OS: ${settings.diagnostic_runtime_os || '(unknown)'}`,
    `  Browser origin: ${browserContext.origin || '(unknown)'}`,
    `  Browser isSecureContext: ${browserContext.is_secure_context == null ? '(not reported)' : browserContext.is_secure_context}`,
    '',
    'Runtime / network topology:',
    '  FastChannels runtime: ' + (topology.fastchannels_runtime || '(unknown)'),
    '  FastChannels OS: ' + (topology.fastchannels_os || '(unknown)'),
    '  FastChannels hostname: ' + (topology.fastchannels_hostname || '(unknown)'),
    '  PrismCast runtime: ' + (topology.prismcast_runtime || '(unknown)'),
    '  PrismCast host: ' + (topology.prismcast_host || '(unknown)'),
    '  PrismCast network mode: ' + (topology.prismcast_network_mode || '(unknown)'),
    '  Watch-page host: ' + (topology.watch_page_host || '(unknown)'),
    '  Watch-page loopback: ' + (topology.watch_page_loopback == null ? '(unknown)' : topology.watch_page_loopback),
    '  Public base host: ' + (topology.public_base_host || '(unknown)'),
    '  Public/watch host match: ' + (topology.public_watch_host_match == null ? '(unknown)' : topology.public_watch_host_match),
    '  Docker socket visible: ' + (topology.docker_socket_available == null ? '(unknown)' : topology.docker_socket_available),
    '  Network-mode source: ' + (topology.network_mode_source || '(unknown)'),
    '  Network-mode note: ' + (topology.network_mode_note || '(none)'),
    '  Timing note: ' + (d.timing_note || '(none)'),
    '',
    'FastChannels resource snapshot:',
    '  Load average: ' + (resources.load_average ? resources.load_average.join(', ') : '(unknown)'),
    '  Memory: ' + (resources.memory_mb && resources.memory_mb.MemAvailable != null ? resources.memory_mb.MemAvailable + ' MB available / ' + (resources.memory_mb.MemTotal || '?') + ' MB total' : '(unknown)'),
    '  Process max RSS: ' + (resources.max_rss_mb == null ? '(unknown)' : resources.max_rss_mb + ' MB'),
    '  Cgroup memory: ' + (resources.cgroup_memory_mb && resources.cgroup_memory_mb.current != null ? resources.cgroup_memory_mb.current + ' MB / ' + resources.cgroup_memory_mb.limit + ' MB' : '(not limited or unavailable)'),
    '  PrismCast streams at end: ' + (resources.prismcast_streams ? resources.prismcast_streams.active + '/' + resources.prismcast_streams.limit : '(unknown)'),
    '',
    'PrismCast health:',
    `  Status: ${health.status || '(unknown)'}`,
    `  Version: ${health.version || '(unknown)'}`,
    `  Chrome: ${health.chrome || '(unknown)'}`,
    `  Capture mode: ${health.captureMode || '(unknown)'}`,
    `  FFmpeg available: ${health.ffmpegAvailable}`,
    `  Initial streams: ${streamText(health)}`,
    '',
    'PrismCast API probes:',
  ];
  if (!apiProbes.length) {
    lines.push('  (none)');
  } else {
    apiProbes.forEach(p => {
      lines.push(`  ${p.path}: HTTP ${p.status_code == null ? '(none)' : p.status_code}, ${p.latency_ms == null ? '?' : p.latency_ms}ms, available ${p.available}, content-type ${p.content_type || '(none)'}`);
      if (p.body) lines.push(`    Body: ${p.body}`);
    });
  }
  lines.push('', 'PrismCast port probes:');
  if (!portProbes.length) {
    lines.push('  (none)');
  } else {
    portProbes.forEach(p => {
      lines.push(`  ${p.label}: ${p.host || '?'}:${p.port} ${p.open ? 'open' : 'closed'} (${p.latency_ms == null ? '?' : p.latency_ms}ms)${p.error ? `, ${p.error}` : ''}`);
    });
  }
  lines.push('', 'PrismCast health snapshots:');
  if (!healthSnapshots.length) {
    lines.push('  (none)');
  } else {
    healthSnapshots.forEach(snap => lines.push(snapshotLine(snap)));
  }
  lines.push('', 'DRM candidates:');
  if (!candidates.length) {
    lines.push('  (none)');
  } else {
    candidates.forEach(c => {
      lines.push(`  ${c.source}: ${c.selected_channel_name ? `selected ${c.selected_channel_name} (#${c.selected_channel_id})` : 'no selected channel'}; active/enabled ${c.total_active_enabled}; bridge candidates ${c.candidate_count}`);
      lines.push(`    Auth: ${c.auth_detail || '(unknown)'}`);
      if (c.test_skipped_reason) lines.push(`    Test: ${c.test_skipped_reason}`);
      if (c.reason) lines.push(`    Readiness: ${c.reason}`);
      if (c.fix) lines.push(`    Fix: ${c.fix}`);
    });
  }
  lines.push('', 'Checks:');
  (data.checks || []).forEach(c => {
    lines.push(`  [${c.status || '?'}] ${c.name || ''}: ${c.detail || ''}`);
    if (c.fix) {
      const label = c.status === 'info' ? 'Note' : 'Fix';
      lines.push(`      ${label}: ${c.fix}`);
    }
  });
  const proxyFailures = d.proxy_failures || [];
  lines.push('', 'Proxy failures during diagnostic:');
  if (!proxyFailures.length) {
    lines.push('  (none)');
  } else {
    proxyFailures.forEach(e => {
      const when = e.timestamp ? new Date(Number(e.timestamp) * 1000).toISOString() : '(unknown time)';
      lines.push('  ' + when + ' ' + (e.label || 'upstream')
        + ' | request_id=' + (e.request_id || '-')
        + ' | upstream_status=' + (e.upstream_status == null ? '?' : e.upstream_status)
        + ' | bytes=' + (e.bytes || 0)
        + ' | ' + (e.error || 'stream failure'));
    });
  }
  lines.push('', 'Capture attempts:');
  if (!attempts.length) {
    lines.push('  (none)');
  } else {
    attempts.forEach((a, i) => {
      lines.push(`  Attempt ${i + 1}: ${a.channel_name || '(unknown)'} (${a.source || '?'}/${a.source_channel_id || a.channel_id || '?'})`);
      lines.push(`    Diagnostic ID: ${a.request_id || '(none)'}`);
      lines.push(`    Watch URL: ${a.watch_url || '(blank)'}`);
      lines.push(`    PrismCast /play URL: ${a.prismcast_play_url || '(blank)'}`);
      lines.push(`    /play status: ${a.play_status == null ? '(none)' : a.play_status}`);
      if (a.failure_class) lines.push(`    Failure classification: ${a.failure_class}`);
      if (a.prismcast_play_elapsed_seconds != null) lines.push(`    PrismCast /play elapsed: ${a.prismcast_play_elapsed_seconds}s`);
      if (a.hls_probe_elapsed_seconds != null) lines.push(`    HLS probe elapsed: ${a.hls_probe_elapsed_seconds}s`);
      if (a.play_body) lines.push(`    /play body: ${a.play_body}`);
      if (a.redirect_url) lines.push(`    HLS redirect: ${a.redirect_url}`);
      if (a.health_before_play) lines.push(`    Health before /play: ${snapshotLine(a.health_before_play).trim()}`);
      if (a.health_after_play) lines.push(`    Health after /play: ${snapshotLine(a.health_after_play).trim()}`);
      if (a.health_after_hls) lines.push(`    Health after HLS: ${snapshotLine(a.health_after_hls).trim()}`);
      lines.push(`    HLS statuses: ${(a.hls_statuses || []).join(', ') || '(none)'}`);
      lines.push(`    Segments: ${a.segments || 0}`);
      if (a.watch_debug) {
        const wd = a.watch_debug || {};
        const state = wd.last_state || {};
        lines.push(`    Watch debug: event ${wd.last_event || '(none)'}, max ready ${wd.max_ready_state == null ? '(unknown)' : wd.max_ready_state}, max decoded ${wd.max_decoded == null ? '(unknown)' : wd.max_decoded}, playback ${wd.browser_playback ? 'true' : 'false'}`);
        if (state.video_size || state.buffered_ahead != null) lines.push(`    Watch state: video ${state.video_size || '(unknown)'}, buffered ahead ${state.buffered_ahead == null ? '(unknown)' : state.buffered_ahead}`);
      }
      lines.push(`    Elapsed seconds: ${a.elapsed_seconds == null ? '(unknown)' : a.elapsed_seconds}`);
    });
  }
  return lines.join('\n');
}

async function copyPrismcastDiagnostics() {
  if (!_lastPrismcastDiagnostics) return;
  const btn = document.getElementById('prismcast-copy-diagnostics');
  try {
    await navigator.clipboard.writeText(_lastPrismcastDiagnostics);
    if (btn) {
      const old = btn.textContent;
      btn.textContent = 'Copied';
      setTimeout(() => { btn.textContent = old; }, 1200);
    }
  } catch (_) {
    window.prompt('Copy diagnostics', _lastPrismcastDiagnostics);
  }
}

async function testPrismcast() {
  const modal = document.getElementById('prismcast-test-modal');
  const body  = document.getElementById('prismcast-test-body');
  const rerun = document.getElementById('prismcast-test-rerun');
  const copyBtn = document.getElementById('prismcast-copy-diagnostics');
  const esc = s => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
  const escAttr = s => esc(s).replace(/'/g, '&#39;');
  modal.classList.add('open');
  rerun.disabled = true;
  if (copyBtn) copyBtn.disabled = true;
  _lastPrismcastDiagnostics = '';
  const loadingSteps = [
    'Checking PrismCast health',
    'Opening the watch page',
    'Starting browser capture',
    'Waiting for HLS segments',
    'Collecting diagnostics',
  ];
  body.innerHTML = '<div class="prismcast-loading" role="status" aria-live="polite">'
    + '<div class="prismcast-loading-copy"><span class="prismcast-loading-spinner" aria-hidden="true"></span>'
    + '<div><div class="prismcast-loading-title">Running checks</div>'
    + '<div class="prismcast-loading-step" id="prismcast-loading-step">' + loadingSteps[0] + '</div></div></div>'
    + '<div class="prismcast-loading-bar" aria-hidden="true"><span></span></div></div>';
  let loadingStepIndex = 0;
  const loadingStepTimer = window.setInterval(() => {
    const el = document.getElementById('prismcast-loading-step');
    if (!el) {
      window.clearInterval(loadingStepTimer);
      return;
    }
    loadingStepIndex = (loadingStepIndex + 1) % loadingSteps.length;
    el.textContent = loadingSteps[loadingStepIndex];
  }, 1800);
  try {
    const r = await fetch('/api/settings/prismcast/test', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ browser_origin: window.location.origin, browser_secure_context: window.isSecureContext }),
    });
    if (r.status === 400) {
      const e = await r.json();
      window.clearInterval(loadingStepTimer);
      body.innerHTML = `<div style="color:var(--warning-soft)">${esc(e.error || 'PrismCast is not configured.')}</div>`;
      return;
    }
    const data = await r.json();
    _lastPrismcastDiagnostics = formatPrismcastDiagnostics(data);
    if (copyBtn) copyBtn.disabled = false;
    const icon  = { ok:'✓', warn:'⚠', fail:'✗', skip:'–', info:'ℹ' };
    const color = { ok:'var(--success-soft)', warn:'var(--warning-soft)', fail:'var(--danger)', skip:'var(--text-muted)', info:'var(--accent-soft)' };
    const overall = (data.overall || 'ok');
    const d = data.diagnostics || {};
    const topology = d.topology || {};
    const resources = d.resources || {};
    const attempts = d.attempts || [];
    const candidates = d.candidates || [];
    const browserContext = d.browser_context || {};
    const apiProbes = d.api_probes || [];
    const portProbes = d.port_probes || [];
    const health = d.health || {};
    const attemptBySource = {};
    attempts.forEach(a => { if (a.source) attemptBySource[a.source] = a; });
    const sourceCandidates = candidates.filter(c => c.source_enabled);
    const disabledSources = candidates.filter(c => !c.source_enabled).map(c => c.source);
    const actionChecks = (data.checks || []).filter(c => (c.status === 'warn' || c.status === 'fail') && !String(c.name || '').includes('candidate readiness'));
    const secureContext = browserContext.is_secure_context === true;
    const summaryItem = (label, value, tone='var(--text-subtle)') =>
      `<div style="min-width:0"><div style="font-size:0.72rem;color:var(--text-muted);text-transform:uppercase">${esc(label)}</div><div style="margin-top:0.18rem;color:${tone};font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(value)}</div></div>`;

    let html = `<div style="display:flex;align-items:center;justify-content:space-between;gap:0.75rem;margin-bottom:0.8rem">`
      + `<div><div style="font-size:1.05rem;font-weight:700;color:${color[overall]||'var(--text)'}">${esc(data.overall_label || overall.toUpperCase())}</div>`
      + `<div style="color:var(--text-muted);font-size:0.82rem;margin-top:0.18rem">Live bridge, browser context, and source checks</div></div>`
      + `<div style="font-size:1.7rem;color:${color[overall]||'var(--text)'}" aria-hidden="true">${icon[overall]||'?'}</div></div>`;

    html += `<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(125px,1fr));gap:0.65rem;padding:0.7rem 0;border-top:1px solid var(--border);border-bottom:1px solid var(--border);margin-bottom:0.8rem">`
      + summaryItem('PrismCast', health.status === 'healthy' ? 'Healthy' : (health.status || 'Unknown'), health.status === 'healthy' ? 'var(--success-soft)' : 'var(--danger)')
      + summaryItem('Browser context', secureContext ? 'Trusted' : 'Not trusted', secureContext ? 'var(--success-soft)' : 'var(--warning-soft)')
      + summaryItem('Chrome', health.chrome || 'Unknown')
      + summaryItem('Capture slots', health.streams ? `${health.streams.active}/${health.streams.limit}` : 'Unknown')
      + `</div>`;

    if (actionChecks.length) {
      html += `<div style="margin-bottom:0.8rem">`;
      actionChecks.forEach(c => {
        html += `<div style="padding:0.6rem 0;border-bottom:1px solid var(--border)">`
          + `<div style="color:${color[c.status]||''};font-weight:600">${icon[c.status]||'?'} ${esc(c.name)}</div>`
          + `<div style="color:var(--text-subtle);font-size:0.86rem;margin-top:0.2rem">${esc(c.detail)}</div>`
          + (c.fix ? `<div style="color:var(--warning-soft);font-size:0.82rem;margin-top:0.25rem">Fix: ${esc(c.fix)}</div>` : '')
          + `</div>`;
      });
      html += `</div>`;
    }

    if (sourceCandidates.length) {
      html += `<div style="font-size:0.8rem;color:var(--text-muted);text-transform:uppercase;margin-bottom:0.35rem">Source results</div>`;
      sourceCandidates.forEach(c => {
        const attempt = attemptBySource[c.source];
        const passed = attempt && (attempt.segments || 0) >= 2;
        const skipped = Boolean(c.test_skipped_reason);
        const notTested = !c.selected_channel_name;
        const state = passed ? 'Passed' : (skipped ? 'Skipped' : (notTested ? 'Not tested' : 'Failed'));
        const stateColor = passed ? 'var(--success-soft)' : (skipped || notTested ? 'var(--warning-soft)' : 'var(--danger)');
        const detail = passed ? `${attempt.segments}+ segments · ${attempt.elapsed_seconds || '?'}s` : (c.test_skipped_reason || c.reason);
        html += `<div style="display:flex;align-items:flex-start;gap:0.55rem;padding:0.62rem 0;border-bottom:1px solid var(--border)">`
          + `<div style="color:${stateColor};font-size:1rem;line-height:1.2">${passed ? '✓' : (skipped || notTested ? '⚠' : '✗')}</div>`
          + `<div style="min-width:0;flex:1"><div style="display:flex;justify-content:space-between;gap:0.5rem;flex-wrap:wrap"><strong>${esc(c.source)}</strong><span style="color:${stateColor};font-size:0.82rem;font-weight:600">${state}</span></div>`
          + `<div style="color:var(--text-subtle);font-size:0.84rem;margin-top:0.2rem">${esc(c.selected_channel_name ? c.selected_channel_name : detail)}</div>`
          + `<div style="color:var(--text-muted);font-size:0.78rem;margin-top:0.15rem">${esc(c.auth_detail || '')}${c.selected_channel_name ? ` · ${esc(detail)}` : ''}</div></div></div>`;
      });
      html += `</div>`;
    }

    if (disabledSources.length) {
      html += `<details style="margin-top:0.75rem"><summary style="cursor:pointer;color:var(--text-muted);font-size:0.82rem">Disabled DRM sources (${disabledSources.length})</summary><div style="color:var(--text-muted);font-size:0.82rem;margin-top:0.45rem">${disabledSources.map(esc).join(' · ')}</div></details>`;
    }

    html += `<details style="margin-top:0.75rem"><summary style="cursor:pointer;color:var(--text-soft);font-weight:600">Technical details</summary>`;
    html += `<div style="margin-top:0.55rem;color:var(--text-subtle);font-size:0.84rem">Watch page: ${esc(d.settings?.watch_page_url_base || '(unknown)')}<br>PrismCast: ${esc(d.settings?.prismcast_url || '(unknown)')}<br>Secure context: ${esc(browserContext.origin || '(unknown)')} · ${secureContext ? 'true' : 'false'}</div>`;
    html += '<div style="margin-top:0.65rem;color:var(--text-muted);font-size:0.8rem">Runtime / network topology: ' + esc(topology.fastchannels_runtime || '(unknown)') + ' · PrismCast runtime ' + esc(topology.prismcast_runtime || '(unknown)') + ' · network mode ' + esc(topology.prismcast_network_mode || '(unknown)') + ' · Watch-page host ' + esc(topology.watch_page_host || '(unknown)') + '</div>';
    html += '<div style="margin-top:0.35rem;color:var(--text-muted);font-size:0.8rem">Timing: ' + esc(d.timing_note || '(unknown)') + '</div>';
    html += '<div style="margin-top:0.35rem;color:var(--text-muted);font-size:0.8rem">Resources: ' + esc(resources.load_average ? 'load ' + resources.load_average.join(', ') : 'load unknown') + ' · ' + esc(resources.memory_mb && resources.memory_mb.MemAvailable != null ? resources.memory_mb.MemAvailable + ' MB available' : 'memory unknown') + ' · max RSS ' + esc(resources.max_rss_mb == null ? 'unknown' : resources.max_rss_mb + ' MB') + '</div>';
    if (apiProbes.length || portProbes.length) {
      html += `<div style="margin-top:0.65rem;color:var(--text-muted);font-size:0.8rem">`
        + apiProbes.map(p => `${esc(p.path)} · HTTP ${esc(p.status_code == null ? '(none)' : p.status_code)}`).join('<br>')
        + (portProbes.length ? `<br>` + portProbes.map(p => `${esc(p.label)} · ${p.open ? 'open' : 'closed'}`).join('<br>') : '')
        + `</div>`;
    }
    html += `</details>`;

    if (attempts.length) {
      html += `<details style="margin-top:0.65rem"><summary style="cursor:pointer;color:var(--text-muted);font-size:0.82rem">Capture details (${attempts.length})</summary>`;
      attempts.forEach((a, i) => {
        html += `<div style="margin-top:0.55rem;padding:0.55rem 0;border-top:1px solid var(--border);font-size:0.8rem;color:var(--text-muted)">`
          + `<strong>${i + 1}. ${esc(a.source || '')} · ${esc(a.channel_name || 'Unknown channel')}</strong><br>`
          + `Diagnostic ID: ${esc(a.request_id || '(none)')} · HTTP ${esc(a.play_status == null ? '(none)' : a.play_status)} · ${esc(a.segments || 0)} segments · ${esc(a.elapsed_seconds == null ? '?' : a.elapsed_seconds)}s`
          + `<br>Class: ${esc(a.failure_class || 'unknown')} · /play ${esc(a.prismcast_play_elapsed_seconds == null ? '?' : a.prismcast_play_elapsed_seconds)}s · HLS ${esc(a.hls_probe_elapsed_seconds == null ? '?' : a.hls_probe_elapsed_seconds)}s`
          + (a.play_body ? `<br><span style="color:var(--warning-soft)">${esc(a.play_body)}</span>` : '')
          + `</div>`;
      });
      html += `</details>`;
    }
    html += `<details style="margin-top:0.65rem"><summary style="cursor:pointer;color:var(--text-muted);font-size:0.82rem">Copyable diagnostics</summary>`
      + `<pre style="white-space:pre-wrap;word-break:break-word;font-size:0.75rem;color:var(--text-muted);background:rgba(255,255,255,0.03);border:1px solid var(--border);border-radius:6px;padding:0.65rem;max-height:220px;overflow:auto;margin-top:0.5rem">${esc(_lastPrismcastDiagnostics)}</pre></details>`;
    window.clearInterval(loadingStepTimer);
    body.innerHTML = html;
  } catch (e) {
    window.clearInterval(loadingStepTimer);
    body.innerHTML = `<div style="color:var(--danger)">Test failed to run: ${esc(e)}</div>`;
  } finally {
    rerun.disabled = false;
  }
}

async function saveDrmBridge() {
  const enabled = document.getElementById('drm-bridge-enabled').checked;
  await saveSettings({ drm_bridge_enabled: enabled }, 'prismcast-status');
  // The toggle reconciles existing DRM channels server-side; reload so the nudge,
  // badges, and counts reflect the new state.
  setTimeout(() => location.reload(), 700);
}

async function testDvrConnection() {
  const statusEl = document.getElementById('dvr-settings-status');
  statusEl.textContent = 'Testing…';
  statusEl.className = 'save-status';
  try {
    const resp = await fetch('/api/dvr/test-connection');
    const data = await resp.json();
    if (data.ok) {
      let msg = `Connected to Channels DVR v${data.version}`;
      if (data.username) msg += ` (${data.username})`;
      statusEl.textContent = msg;
      statusEl.className = 'save-status ok';
    } else {
      statusEl.textContent = data.error || 'Connection failed.';
      statusEl.className = 'save-status error';
    }
  } catch (e) {
    statusEl.textContent = 'Connection failed.';
    statusEl.className = 'save-status error';
  }
}

async function saveDvrSettings() {
  const dvrUrl = document.getElementById('channels-dvr-url').value.trim();
  const autoRefresh = document.getElementById('dvr-epg-auto-refresh').checked;
  const ok = await saveSettings({
    channels_dvr_url: dvrUrl || null,
    dvr_epg_auto_refresh: autoRefresh,
  }, 'dvr-settings-status');
  if (ok) {
    settingsNeedsConfigState.channels_dvr_url = !dvrUrl && !settingsBootstrap.channelsDvrUrlFromEnv;
    refreshSettingsNeedsConfigBanner();
  }
}

async function saveTimezoneSettings() {
  const timezoneName = document.getElementById('tz-input').value.trim();
  // Reject a typed value that isn't an exact IANA name from the list (e.g. a
  // legacy alias like "US/Central") before hitting the server, and point the
  // user at the search so they can pick a canonical name.
  if (timezoneName && typeof window.fcTimezoneIsValid === 'function' && !window.fcTimezoneIsValid(timezoneName)) {
    const status = document.getElementById('timezone-settings-status');
    if (status) {
      status.className = 'save-status error';
      status.textContent = `✕ "${timezoneName}" isn't a valid timezone — pick one from the list (try typing a city or "central", "eastern", …)`;
    }
    document.getElementById('tz-input').focus();
    return false;
  }
  const ok = await saveSettings({
    timezone_name: timezoneName || null,
  }, 'timezone-settings-status');
  if (ok) {
    settingsNeedsConfigState.timezone_name = !timezoneName;
    refreshSettingsNeedsConfigBanner();
  }
}

async function downloadDbBackup() {
  const btn = document.getElementById('backup-db-btn');
  const status = document.getElementById('backup-db-status');
  if (!btn || !status) return;
  if (btn.disabled) return;

  const originalText = btn.textContent;
  btn.disabled = true;
  btn.textContent = 'Compressing…';
  status.className = 'save-status';
  status.textContent = 'Preparing backup…';

  try {
    const r = await fetch('/api/settings/backup-db');
    if (!r.ok) {
      let message = `HTTP ${r.status}`;
      try {
        const payload = await r.json();
        if (payload?.error) message = payload.error;
      } catch (_) {}
      throw new Error(message);
    }

    const blob = await r.blob();
    const objectUrl = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = objectUrl;
    const disposition = r.headers.get('Content-Disposition') || '';
    const match = disposition.match(/filename=\"?([^\";]+)\"?/i);
    link.download = match ? match[1] : 'fastchannels_backup.db.gz';
    document.body.appendChild(link);
    link.click();
    link.remove();
    setTimeout(() => URL.revokeObjectURL(objectUrl), 30_000);

    status.className = 'save-status ok';
    status.textContent = '✓ Backup ready';
    setTimeout(() => { status.textContent = ''; }, 3000);
  } catch (e) {
    status.className = 'save-status error';
    status.textContent = '✕ ' + e.message;
  } finally {
    btn.disabled = false;
    btn.textContent = originalText;
  }
}

function restoreDbFileChosen(input) {
  const label = document.getElementById('restore-db-filename');
  const btn   = document.getElementById('restore-db-btn');
  if (input.files && input.files[0]) {
    label.textContent = input.files[0].name;
    btn.disabled = false;
  } else {
    label.textContent = 'No file chosen';
    btn.disabled = true;
  }
}

async function uploadDbRestore() {
  const input  = document.getElementById('restore-db-file');
  const btn    = document.getElementById('restore-db-btn');
  const status = document.getElementById('restore-db-status');
  if (!input.files || !input.files[0]) return;

  if (!confirm('Replace the current database with this backup? This cannot be undone.')) return;

  const originalText = btn.textContent;
  btn.disabled = true;
  status.className = 'save-status';
  status.textContent = 'Uploading…';

  const form = new FormData();
  form.append('db_file', input.files[0]);

  try {
    const r = await fetch('/api/settings/restore-db', { method: 'POST', body: form });
    const payload = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(payload.error || `HTTP ${r.status}`);
    status.className = 'save-status ok';
    status.textContent = '✓ Restore complete — restart the container to finish.';
  } catch (e) {
    status.className = 'save-status error';
    status.textContent = '✕ ' + e.message;
    btn.disabled = false;
    btn.textContent = originalText;
  }
}

function closeLocalBackups() {
  document.getElementById('local-backups-modal').classList.remove('open');
}

async function openLocalBackups() {
  const modal = document.getElementById('local-backups-modal');
  const list  = document.getElementById('local-backups-list');
  modal.classList.add('open');
  list.textContent = 'Loading…';
  try {
    const r = await fetch('/api/settings/local-backups');
    const payload = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(payload.error || `HTTP ${r.status}`);
    const backups = payload.backups || [];
    if (!backups.length) {
      list.innerHTML = '<div style="color:var(--text-muted)">No server-side backups yet — the first one runs overnight at 3:30&nbsp;AM.</div>';
      return;
    }
    list.innerHTML = backups.map(b => {
      const mb   = (b.size / 1024 / 1024).toFixed(1);
      const when = new Date(b.mtime * 1000).toLocaleString();
      const safe = b.name.replace(/'/g, "\\'");
      return `<div style="display:flex;align-items:center;justify-content:space-between;gap:0.75rem;padding:0.5rem 0;border-bottom:1px solid var(--border)">
        <div>
          <div style="font-size:0.9rem">${when}</div>
          <div style="color:var(--text-muted);font-size:0.8rem">${mb} MB</div>
        </div>
        <button class="btn btn-delete" style="margin:0" onclick="restoreLocalBackup('${safe}')">Restore</button>
      </div>`;
    }).join('');
  } catch (e) {
    list.innerHTML = '<div class="save-status error">✕ ' + e.message + '</div>';
  }
}

async function restoreLocalBackup(name) {
  if (!confirm('Replace the current database with this backup?\n\n' + name + '\n\nThis cannot be undone.')) return;
  const list = document.getElementById('local-backups-list');
  list.innerHTML = '<div style="color:var(--text-muted)">Restoring…</div>';
  try {
    const r = await fetch('/api/settings/restore-local-backup', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    });
    const payload = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(payload.error || `HTTP ${r.status}`);
    list.innerHTML = '<div class="save-status ok">✓ Restore complete — restart the container to finish.</div>';
  } catch (e) {
    list.innerHTML = '<div class="save-status error">✕ ' + e.message + '</div>';
  }
}

async function loadLatestBackup() {
  const snap    = document.getElementById('latest-backup-snapshot');
  const viewAll = document.getElementById('backup-viewall-btn');
  if (!snap) return;
  try {
    const r = await fetch('/api/settings/local-backups');
    const payload = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(payload.error || `HTTP ${r.status}`);
    const backups = payload.backups || [];
    if (!backups.length) {
      snap.innerHTML = '<span class="snap-when" style="color:var(--text-muted)">No auto-backups yet — the first runs tonight at 3:30&nbsp;AM.</span>';
      if (viewAll) { viewAll.disabled = true; viewAll.textContent = 'View all snapshots →'; }
      return;
    }
    const latest = backups[0];
    const mb   = (latest.size / 1024 / 1024).toFixed(1);
    const when = new Date(latest.mtime * 1000).toLocaleString();
    const safe = latest.name.replace(/'/g, "\\'");
    snap.innerHTML = `
      <div>
        <div class="snap-when">${when}</div>
        <div class="snap-meta">${mb} MB · latest snapshot</div>
      </div>
      <button class="btn btn-danger" style="margin:0" onclick="restoreLocalBackup('${safe}')">Restore</button>`;
    if (viewAll) {
      viewAll.disabled = backups.length < 2;
      viewAll.textContent = backups.length < 2
        ? 'View all snapshots →'
        : `View all ${backups.length} snapshots →`;
    }
  } catch (e) {
    snap.innerHTML = '<span class="snap-when" style="color:var(--text-muted)">Could not load auto-backups.</span>';
    if (viewAll) viewAll.disabled = false;  // modal can still try
  }
}

// ── Timezone searchable combobox ───────────────────────────────────────────
(function () {
  const TZ_LIST = settingsBootstrap.timezoneChoices || [];
  const TZ_ALIAS_MAP = {
    'America/New_York': ['eastern', 'east coast', 'new york'],
    'America/Chicago': ['central', 'chicago'],
    'America/Denver': ['mountain', 'denver'],
    'America/Los_Angeles': ['pacific', 'west coast', 'los angeles', 'la'],
    'America/Phoenix': ['arizona', 'phoenix'],
    'America/Anchorage': ['alaska', 'anchorage'],
    'Pacific/Honolulu': ['hawaii', 'honolulu'],
  };
  const TZ_ENTRIES = TZ_LIST.map(tz => {
    const parts = tz.split('/');
    const locationParts = parts.slice(1).flatMap(part => part.split('_'));
    const searchTokens = new Set([
      tz.toLowerCase(),
      tz.toLowerCase().replaceAll('_', ' '),
      ...parts.map(part => part.toLowerCase()),
      ...locationParts.map(part => part.toLowerCase()),
      ...(TZ_ALIAS_MAP[tz] || []),
    ]);
    return {
      value: tz,
      searchText: Array.from(searchTokens).join(' '),
    };
  });

  // Exact-match set so other code (Save) can reject typed-but-invalid values
  // before they ever reach the server.
  const TZ_SET = new Set(TZ_LIST);
  window.fcTimezoneIsValid = (v) => TZ_SET.has(v);

  const input    = document.getElementById('tz-input');
  const dropdown = document.getElementById('tz-dropdown');
  if (!input || !dropdown) return;

  let activeIdx = -1;

  function getItems() { return dropdown.querySelectorAll('.tz-option'); }

  function setActive(idx) {
    const items = getItems();
    items.forEach((el, i) => el.classList.toggle('active', i === idx));
    activeIdx = idx;
    if (idx >= 0 && items[idx]) {
      items[idx].scrollIntoView({ block: 'nearest' });
    }
  }

  function renderDropdown(query) {
    const q = query.trim().toLowerCase();
    const matches = q
      ? TZ_ENTRIES.filter(entry => entry.searchText.includes(q)).slice(0, 100)
      : TZ_ENTRIES.slice(0, 100);

    dropdown.innerHTML = '';
    activeIdx = -1;

    if (matches.length === 0) {
      const el = document.createElement('div');
      el.className = 'tz-no-results';
      el.textContent = 'No timezones match';
      dropdown.appendChild(el);
    } else {
      matches.forEach((entry, i) => {
        const el = document.createElement('div');
        el.className = 'tz-option';
        el.textContent = entry.value;
        el.dataset.value = entry.value;
        el.addEventListener('mousedown', e => {
          e.preventDefault(); // keep focus on input
          input.value = entry.value;
          closeDropdown();
        });
        dropdown.appendChild(el);
      });
    }
  }

  function openDropdown() {
    renderDropdown(input.value);
    dropdown.hidden = false;
  }

  function closeDropdown() {
    dropdown.hidden = true;
    activeIdx = -1;
  }

  input.addEventListener('focus', () => openDropdown());

  input.addEventListener('input', () => {
    renderDropdown(input.value);
    dropdown.hidden = false;
  });

  input.addEventListener('keydown', e => {
    if (dropdown.hidden) {
      if (e.key === 'ArrowDown' || e.key === 'Enter') { openDropdown(); e.preventDefault(); }
      return;
    }
    const items = getItems();
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setActive(Math.min(activeIdx + 1, items.length - 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setActive(Math.max(activeIdx - 1, 0));
    } else if (e.key === 'Enter') {
      e.preventDefault();
      if (activeIdx >= 0 && items[activeIdx]) {
        input.value = items[activeIdx].dataset.value;
      }
      closeDropdown();
    } else if (e.key === 'Escape') {
      closeDropdown();
    }
  });

  document.addEventListener('click', e => {
    if (!document.getElementById('tz-combobox').contains(e.target)) {
      closeDropdown();
    }
  });
})();

// ── Gracenote auto-fill toggle ─────────────────────────────────────────────
function onGracenoteAutoFillChange(checkbox) {
  saveSettings({ gracenote_auto_fill: checkbox.checked }, 'gracenote-auto-fill-status');
}

async function disableAllGracenote() {
  document.getElementById('gracenote-clear-modal').classList.remove('open');
  const status = document.getElementById('gracenote-auto-fill-status');
  status.className = 'save-status';
  status.textContent = 'Clearing…';
  try {
    const r = await fetch('/api/settings/gracenote-clear-all', { method: 'POST' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    document.getElementById('gracenote-auto-fill-toggle').checked = false;
    status.className = 'save-status ok';
    status.textContent = '✓ Disabled — IDs being cleared in background';
    setTimeout(() => { status.textContent = ''; }, 4000);
  } catch (e) {
    status.className = 'save-status error';
    status.textContent = '✕ ' + e.message;
  }
}

function cancelGracenoteClear() {
  document.getElementById('gracenote-clear-modal').classList.remove('open');
}


// ── System Stats ──────────────────────────────────────────────────────────
function _fmt_bytes(b) {
  if (b >= 1073741824) return (b / 1073741824).toFixed(1) + ' GB';
  if (b >= 1048576)    return (b / 1048576).toFixed(1) + ' MB';
  if (b >= 1024)       return (b / 1024).toFixed(0) + ' KB';
  return b + ' B';
}
function _fmt_uptime(s) {
  const d = Math.floor(s / 86400), h = Math.floor((s % 86400) / 3600), m = Math.floor((s % 3600) / 60);
  if (d > 0) return `${d}d ${h}h ${m}m`;
  if (h > 0) return `${h}h ${m}m`;
  return `${m}m`;
}
function _fmt_seconds(s) {
  if (s == null) return 'Unavailable';
  if (s >= 3600) return `${(s / 3600).toFixed(1)} h`;
  if (s >= 60) return `${(s / 60).toFixed(1)} m`;
  return `${s.toFixed ? s.toFixed(1) : s} s`;
}
function _stat(label, value, cls = '') {
  return `<div class="stat-row"><span class="stat-label">${label}</span><span class="stat-value ${cls}">${value}</span></div>`;
}

async function loadSystemStats() {
  const body = document.getElementById('system-stats-body');
  try {
    const r = await fetch('/api/system-stats');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();

    const db = d.db, ic = d.image_cache, mem = d.memory || {}, cpu = d.cpu || {}, proc = d.processes || {};
    const totalCacheBytes = ic.logos_bytes + ic.posters_bytes;
    const containerUsage = mem.container_bytes ? _fmt_bytes(mem.container_bytes) : 'Unavailable';
    const containerLimit = mem.container_limit_bytes ? _fmt_bytes(mem.container_limit_bytes) : null;
    const anonBytes = mem.container_anon_bytes ? _fmt_bytes(mem.container_anon_bytes) : 'Unavailable';
    const fileCacheBytes = mem.container_file_cache_bytes ? _fmt_bytes(mem.container_file_cache_bytes) : 'Unavailable';
    const processRss = mem.process_rss_bytes ? _fmt_bytes(mem.process_rss_bytes) : 'Unavailable';
    const processSwap = mem.process_swap_bytes ? _fmt_bytes(mem.process_swap_bytes) : '0 B';
    const hostAvailable = mem.host_mem_available_bytes ? _fmt_bytes(mem.host_mem_available_bytes) : 'Unavailable';
    const loadAvg = Array.isArray(cpu.loadavg) ? cpu.loadavg.map(v => v.toFixed(2)).join(' / ') : 'Unavailable';

    body.innerHTML = `
      <div class="stats-sections">
        <div>
          <div class="stats-section-label">Database</div>
          ${_stat('File size', _fmt_bytes(db.size_bytes))}
          ${_stat('Channels (active)', `${db.channels_active.toLocaleString()} / ${db.channels_total.toLocaleString()}`)}
          ${_stat('DRM flagged', db.channels_drm > 0 ? db.channels_drm.toLocaleString() : '0', db.channels_drm > 0 ? 'warn' : 'muted')}
          ${_stat('Dead flagged', db.channels_dead > 0 ? db.channels_dead.toLocaleString() : '0', db.channels_dead > 0 ? 'warn' : 'muted')}
          ${_stat('EPG programs', db.programs_total.toLocaleString())}
          ${_stat('Sources enabled', `${db.sources_enabled} / ${db.sources_total}`)}
        </div>
        <div>
          <div class="stats-section-label">Image Cache</div>
          ${_stat('Logos', `${ic.logos_count.toLocaleString()} files / ${_fmt_bytes(ic.logos_bytes)}`)}
          ${_stat('Logo expiry', ic.logo_expiry || 'url-change')}
          ${_stat('Posters', `${ic.posters_count.toLocaleString()} files / ${_fmt_bytes(ic.posters_bytes)}`)}
          ${_stat('Poster TTL', `${ic.poster_ttl_days} days`)}
          ${_stat('Total cache', _fmt_bytes(totalCacheBytes), totalCacheBytes > 2147483648 ? 'warn' : '')}
        </div>
        <div>
          <div class="stats-section-label">Memory</div>
          ${_stat('Container total (incl. cache)', containerLimit ? `${containerUsage} / ${containerLimit}` : containerUsage, (mem.container_percent || 0) >= 80 ? 'warn' : '')}
          ${_stat('Process memory', anonBytes)}
          ${_stat('File cache (reclaimable)', fileCacheBytes, 'muted')}
          ${_stat('Container %', mem.container_percent != null ? `${mem.container_percent}%` : 'Unavailable', (mem.container_percent || 0) >= 80 ? 'warn' : 'muted')}
          ${_stat('Web process RSS', processRss)}
          ${_stat('Web process swap', processSwap, (mem.process_swap_bytes || 0) > 0 ? 'warn' : 'muted')}
          ${_stat('Host available', hostAvailable)}
        </div>
        <div>
          <div class="stats-section-label">CPU</div>
          ${_stat('Load avg', loadAvg)}
          ${_stat('CPU cores', cpu.cpu_count != null ? cpu.cpu_count.toLocaleString() : 'Unavailable')}
          ${_stat('Web CPU time', _fmt_seconds(cpu.process_cpu_seconds))}
        </div>
        <div>
          <div class="stats-section-label">Processes</div>
          ${_stat('Web workers', proc.web_worker_count != null ? proc.web_worker_count.toLocaleString() : 'Unavailable')}
          ${_stat('Web RSS avg', proc.web_worker_rss_avg_bytes ? _fmt_bytes(proc.web_worker_rss_avg_bytes) : 'Unavailable')}
          ${_stat('Background workers', proc.background_worker_count != null ? proc.background_worker_count.toLocaleString() : 'Unavailable')}
          ${_stat('Background RSS avg', proc.background_worker_rss_avg_bytes ? _fmt_bytes(proc.background_worker_rss_avg_bytes) : 'Unavailable')}
        </div>
      </div>
      <div class="stats-uptime">Uptime: ${_fmt_uptime(d.uptime_seconds)}</div>`;
  } catch(e) {
    body.innerHTML = `<span style="color:var(--danger);font-size:0.85rem">Failed to load stats: ${e.message}</span>`;
  }
}

async function saveContributionUrl() {
  const url = document.getElementById('gn-contribution-url').value.trim();
  const st = document.getElementById('gn-contribution-status');
  st.textContent = 'Saving…'; st.className = 'save-status';
  try {
    const cr = await fetch('/api/settings', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({gracenote_contribution_url: url}) });
    if (!cr.ok) throw new Error(`HTTP ${cr.status}`);
    st.textContent = 'Saved'; st.className = 'save-status saved';
  } catch(e) { st.textContent = 'Error'; st.className = 'save-status error'; }
  setTimeout(() => { st.textContent = ''; }, 3000);
}

initSettingsSectionNav();
loadSystemStats();
updateTveProviderFields();

// ── Community Gracenote map remote status ───────────────────────────────────
async function loadRemoteGracenoteStatus() {
  try {
    const r = await fetch('/api/gracenote/remote-map/status');
    const d = await r.json();
    const urlInput = document.getElementById('gn-map-url');
    const meta = document.getElementById('gn-remote-meta');
    if (urlInput) urlInput.value = d.url_is_default ? '' : (d.url || '');
    if (urlInput) urlInput.placeholder = d.url || 'Default Gist URL';
    if (meta) {
      const ts = d.fetched_at ? new Date(d.fetched_at * 1000).toLocaleString() : null;
      const count = d.row_count ? `${d.row_count.toLocaleString()} rows` : 'not yet fetched';
      const last = ts ? ` · Last fetched ${ts}` : '';
      meta.textContent = `${count}${last}`;
    }
  } catch(e) { /* silently ignore */ }
}

async function saveGracenoteMapUrl() {
  const url = (document.getElementById('gn-map-url')?.value || '').trim();
  const status = document.getElementById('gn-remote-status');
  status.className = 'save-status'; status.textContent = 'Saving…';
  try {
    const r = await fetch('/api/settings', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ gracenote_map_url: url }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    status.className = 'save-status ok'; status.textContent = '✓ Saved';
    setTimeout(() => { status.textContent = ''; }, 3000);
    loadRemoteGracenoteStatus();
  } catch(e) {
    status.className = 'save-status error'; status.textContent = '✕ ' + e.message;
  }
}

async function refreshRemoteGracenoteMap() {
  const btn = document.getElementById('gn-remote-refresh-btn');
  const status = document.getElementById('gn-remote-status');
  if (btn) { btn.disabled = true; btn.textContent = 'Fetching…'; }
  status.className = 'save-status';
  status.textContent = '';
  try {
    const r = await fetch('/api/gracenote/remote-map/refresh', { method: 'POST' });
    const d = await r.json();
    if (!d.ok) throw new Error(d.message || `HTTP ${r.status}`);
    status.className = 'save-status ok';
    status.textContent = '✓ ' + d.message;
    loadRemoteGracenoteStatus();
  } catch(e) {
    status.className = 'save-status error';
    status.textContent = '✕ ' + e.message;
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Refresh Now'; }
    setTimeout(() => { status.textContent = ''; }, 5000);
  }
}

loadRemoteGracenoteStatus();
loadLatestBackup();
