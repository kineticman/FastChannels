// FastChannels Player devices card (admin/bridge/devices.html).
// List is fetched without adb, then every device is probed in parallel so one
// unreachable stick (a full adb timeout) doesn't hold up the others.

const bridgeDevices = new Map();   // address -> {info, probe, busy, message, messageOk}
let bridgeDevicesBundled = null;
let bridgeDevicesLoadSeq = 0;

function _bdPost(url, body) {
  return fetch(url, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body),
  }).then(async (resp) => ({status: resp.status, data: await resp.json().catch(() => ({}))}));
}

function _bdStatus(text, cls = '') {
  const el = document.getElementById('fc-devices-status');
  if (!el) return;
  el.textContent = text || '';
  el.className = 'save-status ' + cls;
}

function _bdAgo(iso) {
  if (!iso) return '';
  const secs = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  if (secs < 90) return 'just now';
  if (secs < 5400) return `${Math.round(secs / 60)} min ago`;
  if (secs < 129600) return `${Math.round(secs / 3600)} h ago`;
  return `${Math.round(secs / 86400)} d ago`;
}

const BD_STATE_LABELS = {
  checking: 'Checking…',
  device: 'Online',
  unauthorized: 'Not authorized',
  offline: 'Offline',
  unreachable: 'Unreachable',
};

function _bdTitle(entry) {
  const info = entry.info;
  const probe = entry.probe || {};
  return info.label || probe.device_name || probe.model || info.host;
}

function _bdPlayerFact(probe) {
  if (probe.player_installed === false) return '<span class="fc-dev-warn">Not installed</span>';
  if (!probe.player_installed) return '<span class="fc-dev-muted">Unknown</span>';
  const v = _escapeHtml(probe.player_version || 'version unknown');
  if (probe.update_available) {
    return `${v} <span class="fc-dev-warn">→ ${_escapeHtml(probe.bundled_version || 'newer')} available</span>`;
  }
  if (probe.update_available === false) return `${v} <span class="fc-dev-ok">up to date</span>`;
  return v;
}

function _bdActivityFact(entry) {
  const probe = entry.probe || {};
  const info = entry.info;
  if (probe.player_playing) {
    // Below 720p is what a "soft picture" report looks like from here.
    const res = probe.video_height
      ? ` · <span class="${probe.video_height < 720 ? 'fc-dev-warn' : 'fc-dev-muted'}">${probe.video_height}p</span>`
      : '';
    return `<span class="fc-dev-ok">▶ Playing</span> ${_escapeHtml(probe.now_playing || '')}${res}`;
  }
  const idle = probe.authorized ? 'Idle' : '';
  if (info.last_tuned_at) {
    const last = `last tuned ${_bdAgo(info.last_tuned_at)}${info.last_channel_name ? ` · ${_escapeHtml(info.last_channel_name)}` : ''}`;
    return idle ? `${idle} · ${last}` : last;
  }
  return idle || '<span class="fc-dev-muted">Never tuned</span>';
}

function _bdNetworkFact(probe) {
  if (probe.network === 'ethernet') return 'Ethernet';
  if (probe.network !== 'wifi') return null;
  const rssi = probe.wifi_rssi;
  // -60 dBm and up holds a 1080p stream comfortably; below -70 brief quality dips are expected.
  const [label, cls] = rssi >= -60 ? ['good', 'fc-dev-ok'] : rssi >= -70 ? ['fair', ''] : ['weak', 'fc-dev-warn'];
  const link = probe.wifi_link_mbps > 0 ? ` · ${probe.wifi_link_mbps} Mbps` : '';
  return `Wi-Fi <span class="${cls}">${rssi} dBm (${label})</span>${link}`;
}

function _bdRecoveryFact(probe) {
  if (!probe.recoveries_last_hour) return null;
  const n = probe.recoveries_last_hour;
  const last = `last: ${_escapeHtml(probe.last_recovery || 'recovery')} ${_bdAgo(probe.last_recovery_at)}`;
  return `<span class="${probe.last_recovery_gave_up ? 'fc-dev-warn' : ''}">${n} in the last hour · ${last}</span>`;
}

function _bdRender(address) {
  const entry = bridgeDevices.get(address);
  const el = document.querySelector(`.fc-device[data-address="${CSS.escape(address)}"]`);
  if (!entry || !el) return;
  const info = entry.info;
  const probe = entry.probe;
  const state = probe ? probe.state : 'checking';
  const facts = [];
  if (probe && probe.authorized) {
    facts.push(['Player', _bdPlayerFact(probe)]);
    if (probe.os_label) facts.push(['OS', _escapeHtml(probe.os_label)]);
    const screen = probe.awake === true ? 'Awake' : probe.awake === false ? 'Asleep' : 'Unknown';
    facts.push(['Screen', `${screen}${probe.focus_app ? ` · ${_escapeHtml(probe.focus_app)} in front` : ''}`]);
    if (probe.sleep_disabled === true) {
      facts.push(['Auto-sleep', '<span class="fc-dev-ok">Off</span>']);
    } else if (probe.sleep_disabled === false) {
      facts.push(['Auto-sleep', `<span class="fc-dev-warn">On</span> <span class="fc-dev-muted">${_escapeHtml(probe.sleep_detail || '')}</span>`]);
    }
  }
  facts.push(['Activity', _bdActivityFact(entry)]);
  if (probe && probe.authorized) {
    const network = _bdNetworkFact(probe);
    if (network) facts.push(['Network', network]);
    const recovery = _bdRecoveryFact(probe);
    if (recovery) facts.push(['Recovery', recovery]);
  }

  const subtitle = [info.host];
  if (probe && probe.model && _bdTitle(entry) !== probe.model) subtitle.push(probe.model);

  const actions = [];
  if (probe && probe.authorized) {
    if (probe.player_installed === false) {
      actions.push(`<button class="btn btn-primary btn-sm" data-act="install" ${entry.busy ? 'disabled' : ''}>Install player</button>`);
    } else if (probe.update_available) {
      actions.push(`<button class="btn btn-primary btn-sm" data-act="install" ${entry.busy ? 'disabled' : ''}>Update to ${_escapeHtml(probe.bundled_version || 'latest')}</button>`);
    } else if (probe.player_installed) {
      actions.push(`<button class="btn btn-secondary btn-sm" data-act="install" ${entry.busy ? 'disabled' : ''}>Reinstall</button>`);
    }
  }
  actions.push('<button class="btn btn-secondary btn-sm" data-act="rename">Rename</button>');
  if (info.remembered && !info.roles.length) {
    actions.push('<button class="btn btn-secondary btn-sm" data-act="forget">Forget</button>');
  }

  let message = '';
  if (entry.message) {
    message = `<div class="fc-device-msg ${entry.messageOk ? 'ok' : 'error'}">${_escapeHtml(entry.message)}</div>`;
  } else if (probe && !probe.authorized && probe.message) {
    message = `<div class="fc-device-msg">${_escapeHtml(probe.message)}</div>`;
  }

  el.className = `fc-device state-${state}`;
  el.innerHTML = `
    <div class="fc-device-head">
      <span class="fc-device-dot" aria-hidden="true"></span>
      <div class="fc-device-ident">
        <div class="fc-device-name">${_escapeHtml(_bdTitle(entry))}</div>
        <div class="fc-device-sub">${subtitle.map(_escapeHtml).join(' · ')}</div>
      </div>
      <span class="fc-device-state">${entry.busy ? 'Installing…' : BD_STATE_LABELS[state] || _escapeHtml(state)}</span>
    </div>
    ${info.roles.length ? `<div class="fc-device-roles">${info.roles.map((r) => `<span class="bridge-source-chip bridge-source-chip-static">${_escapeHtml(r)}</span>`).join('')}</div>` : ''}
    <dl class="fc-device-facts">${facts.map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`).join('')}</dl>
    ${message}
    <div class="fc-device-actions">${actions.join('')}</div>`;
}

function _bdUpdateToolbar() {
  const outdated = [...bridgeDevices.values()].filter((e) => e.probe && e.probe.update_available);
  const btn = document.getElementById('fc-devices-update-all');
  if (btn) {
    btn.hidden = outdated.length < 2;
    btn.textContent = `Update all (${outdated.length})`;
  }
  const bundled = document.getElementById('fc-devices-bundled');
  if (bundled) {
    bundled.textContent = bridgeDevicesBundled
      ? `This FastChannels version ships FastChannels Player ${bridgeDevicesBundled}.`
      : 'No FastChannels Player release is bundled in this build.';
  }
}

async function _bdProbe(address, seq) {
  try {
    const {data} = await _bdPost('/api/settings/fc-player/devices/probe', {address});
    if (seq !== bridgeDevicesLoadSeq) return;
    const entry = bridgeDevices.get(address);
    if (!entry) return;
    entry.probe = data.ok ? data.device : {state: 'unreachable', message: data.message || 'Probe failed.'};
  } catch (e) {
    if (seq !== bridgeDevicesLoadSeq) return;
    const entry = bridgeDevices.get(address);
    if (entry) entry.probe = {state: 'unreachable', message: 'Probe failed.'};
  }
  _bdRender(address);
  _bdUpdateToolbar();
}

async function loadBridgeDevices() {
  const list = document.getElementById('fc-devices-list');
  if (!list) return;
  const seq = ++bridgeDevicesLoadSeq;
  _bdStatus('Loading devices…');
  let data;
  try {
    data = await fetch('/api/settings/fc-player/devices').then((r) => r.json());
  } catch (e) {
    _bdStatus('✕ Could not load devices.', 'error');
    return;
  }
  if (seq !== bridgeDevicesLoadSeq) return;
  bridgeDevicesBundled = data.bundled_version || null;
  bridgeDevices.clear();
  list.innerHTML = '';
  for (const info of data.devices || []) {
    bridgeDevices.set(info.address, {info, probe: null, busy: false, message: '', messageOk: true});
    const el = document.createElement('div');
    el.className = 'fc-device state-checking';
    el.dataset.address = info.address;
    list.appendChild(el);
    _bdRender(info.address);
  }
  _bdUpdateToolbar();
  if (!bridgeDevices.size) {
    list.innerHTML = '<div class="fc-devices-empty">No devices yet. Set an HDMI Capture device IP, connect ah4c, or add a device here.</div>';
  }
  _bdStatus(data.ah4c_error || '', data.ah4c_error ? 'error' : '');
  for (const address of bridgeDevices.keys()) _bdProbe(address, seq);
}

async function _bdInstall(address, {force = false, silent = false} = {}) {
  const entry = bridgeDevices.get(address);
  if (!entry || entry.busy) return false;
  entry.busy = true;
  entry.message = '';
  _bdRender(address);
  let result;
  try {
    result = await _bdPost('/api/settings/fc-player/devices/install', {address, force});
  } catch (e) {
    result = {status: 0, data: {ok: false, message: 'Install request failed.'}};
  }
  entry.busy = false;
  if (result.data.busy && !force) {
    if (!silent && confirm(`${_bdTitle(entry)}: ${result.data.message}\n\nUpdate anyway?`)) {
      return _bdInstall(address, {force: true});
    }
    entry.message = silent ? 'Skipped — playing right now.' : 'Update cancelled — device is playing.';
    entry.messageOk = false;
    _bdRender(address);
    return false;
  }
  entry.message = result.data.message || (result.data.ok ? 'Installed.' : 'Install failed.');
  entry.messageOk = !!result.data.ok;
  _bdRender(address);
  if (result.data.ok) await _bdProbe(address, bridgeDevicesLoadSeq);
  return !!result.data.ok;
}

async function updateAllBridgeDevices() {
  const targets = [...bridgeDevices.values()]
    .filter((e) => e.probe && e.probe.update_available)
    .map((e) => e.info.address);
  if (!targets.length) return;
  if (!confirm(`Update FastChannels Player on ${targets.length} device(s)? Devices that are playing right now are skipped.`)) return;
  let done = 0;
  // One at a time, so a slow or failing device is easy to pick out.
  for (const address of targets) {
    _bdStatus(`Updating ${done + 1} of ${targets.length}…`);
    if (await _bdInstall(address, {silent: true})) done += 1;
  }
  _bdStatus(`Updated ${done} of ${targets.length} device(s).`, done === targets.length ? 'ok' : 'error');
}

async function _bdRename(address) {
  const entry = bridgeDevices.get(address);
  if (!entry) return;
  const label = prompt('Device name (leave blank to use the name the device reports):', entry.info.label || '');
  if (label === null) return;
  const {data} = await _bdPost('/api/settings/fc-player/devices', {address, label});
  if (!data.ok) {
    entry.message = data.message || 'Rename failed.';
    entry.messageOk = false;
  } else {
    entry.info.label = label.trim() || null;
    entry.info.remembered = true;
  }
  _bdRender(address);
}

async function _bdForget(address) {
  const entry = bridgeDevices.get(address);
  if (!entry || !confirm(`Forget ${_bdTitle(entry)}? It reappears if FastChannels tunes it again.`)) return;
  const {data} = await _bdPost('/api/settings/fc-player/devices/forget', {address});
  if (data.ok) {
    loadBridgeDevices();
  } else {
    entry.message = data.message || 'Could not forget device.';
    entry.messageOk = false;
    _bdRender(address);
  }
}

function toggleBridgeDeviceAdd() {
  const box = document.getElementById('fc-devices-add');
  box.hidden = !box.hidden;
  if (!box.hidden) document.getElementById('fc-devices-add-address').focus();
}

async function addBridgeDevice() {
  const addressEl = document.getElementById('fc-devices-add-address');
  const labelEl = document.getElementById('fc-devices-add-label');
  const {data} = await _bdPost('/api/settings/fc-player/devices', {
    address: addressEl.value, label: labelEl.value, add: true,
  });
  if (!data.ok) {
    _bdStatus('✕ ' + (data.message || 'Could not add device.'), 'error');
    return;
  }
  addressEl.value = '';
  labelEl.value = '';
  document.getElementById('fc-devices-add').hidden = true;
  loadBridgeDevices();
}

document.addEventListener('click', (event) => {
  const btn = event.target.closest('.fc-device [data-act]');
  if (!btn) return;
  const address = btn.closest('.fc-device').dataset.address;
  const act = btn.dataset.act;
  if (act === 'install') _bdInstall(address);
  else if (act === 'rename') _bdRename(address);
  else if (act === 'forget') _bdForget(address);
});

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('fc-devices-list')) loadBridgeDevices();
});
