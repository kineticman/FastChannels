// community_map.js — Shared Community Map + Gracenote Contribution logic

let _cmData = [];

function escCm(s) {
  return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function cmStatusInfo(row) {
  if (row.gracenote_mode === 'off') return { label: 'Off', cls: 'off', key: 'off' };
  if (row.already_applied) return { label: 'Applied', cls: 'applied', key: 'applied' };
  if (row.gracenote_mode === 'manual') return { label: 'Manual', cls: 'manual', key: 'manual' };
  if (row.current_id && !row.already_applied) return { label: 'Has native ID', cls: 'native', key: 'native' };
  return { label: 'Available', cls: 'none', key: 'available' };
}

function renderCmTable() {
  const search = (document.getElementById('cm-search').value || '').toLowerCase();
  const filterStatus = document.getElementById('cm-filter-status').value;
  const filterSource = document.getElementById('cm-filter-source').value;
  const filterCategory = document.getElementById('cm-filter-category').value;
  let rows = _cmData.filter(row => {
    if (search && !row.channel_name.toLowerCase().includes(search) && !row.source_name.toLowerCase().includes(search)) return false;
    if (filterSource && row.source_name !== filterSource) return false;
    if (filterCategory && row.category !== filterCategory) return false;
    if (filterStatus && cmStatusInfo(row).key !== filterStatus) return false;
    return true;
  });
  document.getElementById('cm-count').textContent = `${rows.length} channel${rows.length !== 1 ? 's' : ''}`;
  const tbody = document.getElementById('cm-tbody');
  if (!rows.length) { tbody.innerHTML = '<tr><td colspan="7" class="cm-empty">No matches.</td></tr>'; return; }
  tbody.innerHTML = rows.map(row => {
    const s = cmStatusInfo(row);
    const alreadyApplied = s.key === 'applied';
    return `
      <tr id="cm-row-${row.channel_id}">
        <td>${escCm(row.channel_name)}</td>
        <td>${escCm(row.source_name)}</td>
        <td style="color:#94a3b8;font-size:0.8rem">${escCm(row.category || '—')}</td>
        <td><code style="font-size:0.8rem">${escCm(row.community_tmsid)}</code></td>
        <td style="color:#64748b;font-size:0.8rem">${escCm(row.notes)}</td>
        <td><span class="cm-badge ${escCm(s.cls)}">${escCm(s.label)}</span></td>
        <td><button class="btn-cm-apply" id="cm-btn-${row.channel_id}"
          onclick="openGracenoteSuggestModal(${row.channel_id}, {onApplied: _cmGnOnApplied})"
          ${alreadyApplied ? 'disabled' : ''}
        >${alreadyApplied ? 'Applied' : 'Review &amp; Apply'}</button></td>
      </tr>`;
  }).join('');
}

async function openCommunityMap() {
  document.getElementById('cm-overlay').classList.add('open');
  if (_cmData.length) return;
  try {
    const r = await fetch('/api/gracenote/community-map');
    const data = await r.json();
    _cmData = data.results || [];
    const sources = [...new Set(_cmData.map(r => r.source_name))].sort();
    const sel = document.getElementById('cm-filter-source');
    sources.forEach(s => { const o = document.createElement('option'); o.value = s; o.textContent = s; sel.appendChild(o); });
    const categories = [...new Set(_cmData.map(r => r.category).filter(Boolean))].sort();
    const catSel = document.getElementById('cm-filter-category');
    categories.forEach(c => { const o = document.createElement('option'); o.value = c; o.textContent = c; catSel.appendChild(o); });
    renderCmTable();
    // First-time guidance: if nothing is applied yet, show a "get started" banner
    const noneApplied = _cmData.length > 0 && _cmData.every(r => cmStatusInfo(r).key === 'available');
    if (noneApplied && !document.getElementById('cm-firstrun-banner')) {
      const banner = document.createElement('div');
      banner.id = 'cm-firstrun-banner';
      banner.style.cssText = 'background:#0d1f38;border:1px solid #1d4ed8;border-radius:8px;padding:0.75rem 1rem;margin-bottom:0.75rem;font-size:0.875rem;line-height:1.55;color:#cbd5e1';
      banner.innerHTML = `<strong style="color:#7dd3fc">New here?</strong> The community has pre-mapped <strong>${_cmData.length}</strong> channels. Click <strong>Apply All</strong> above to apply them all at once, or filter and apply individually below.`;
      const filters = document.querySelector('#cm-overlay .cm-filters');
      if (filters) filters.parentNode.insertBefore(banner, filters);
    }
  } catch(e) {
    document.getElementById('cm-tbody').innerHTML = `<tr><td colspan="7" class="cm-empty">Failed to load: ${escCm(String(e))}</td></tr>`;
  }
}

function closeCommunityMap() {
  document.getElementById('cm-overlay').classList.remove('open');
}

function closeApplyAllConfirm() {
  document.getElementById('cm-apply-all-modal').classList.remove('open');
}

async function openApplyAllConfirm() {
  const modal = document.getElementById('cm-apply-all-modal');
  const content = document.getElementById('cm-apply-all-content');
  content.innerHTML = '<div class="modal-loading">Checking…</div>';
  modal.classList.add('open');

  try {
    const r = await fetch('/api/gracenote/community-apply-all', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dry_run: true }),
    });
    const d = await r.json();

    const cleanCount  = d.applied.length;
    const overCount   = d.overwritten.length;
    const doneCount   = d.already_done;

    const overList = d.overwritten.length
      ? `<div style="margin-top:0.75rem;max-height:160px;overflow-y:auto;border:1px solid #334155;border-radius:6px;padding:0.4rem 0.6rem">
          ${d.overwritten.map(r =>
            `<div style="font-size:0.8rem;padding:0.2rem 0;border-bottom:1px solid #1e293b;color:#f87171">
              <strong>${escCm(r.channel_name)}</strong>
              <span style="color:#64748b"> · ${escCm(r.source_name)} · current: ${escCm(r.current_id || '—')} (${escCm(r.mode)})</span>
            </div>`
          ).join('')}
        </div>` : '';

    content.innerHTML = `
      <h3 style="margin-bottom:0.75rem;font-size:1rem">Apply All Community IDs</h3>
      <div style="font-size:0.875rem;color:#cbd5e1;line-height:1.6;margin-bottom:0.75rem">
        <div>✅ <strong>${cleanCount}</strong> channel${cleanCount !== 1 ? 's' : ''} will get the community ID applied</div>
        ${overCount ? `<div style="color:#f87171">⚠️ <strong>${overCount}</strong> channel${overCount !== 1 ? 's' : ''} with a Manual or Off override will be overwritten</div>` : ''}
        <div style="color:#475569">${doneCount} already applied — will be skipped</div>
      </div>
      ${overCount ? `<div style="font-size:0.8rem;color:#94a3b8;margin-bottom:0.3rem">Channels that will be overwritten:</div>${overList}` : ''}
      ${cleanCount + overCount === 0
        ? `<div style="background:#0d1f12;border:1px solid #14532d;border-radius:8px;padding:0.75rem 1rem;font-size:0.875rem;color:#86efac">✅ All ${doneCount} community ID${doneCount !== 1 ? 's' : ''} are already applied — you're fully up to date!</div>
           <div style="display:flex;justify-content:flex-end;margin-top:1rem">
             <button class="btn-sm btn-secondary" onclick="closeApplyAllConfirm()">Close</button>
           </div>`
        : `<div style="display:flex;gap:0.5rem;justify-content:flex-end;margin-top:1rem;flex-wrap:wrap">
             <button class="btn-sm btn-secondary" onclick="closeApplyAllConfirm()">Cancel</button>
             ${cleanCount > 0 ? `<button class="btn-sm" onclick="runApplyAll(true)" style="background:#0f766e;color:#fff">Apply New Only (${cleanCount})</button>` : ''}
             ${overCount > 0 ? `<button class="btn-sm" onclick="runApplyAll(false)" style="background:#b45309;color:#fff" title="Also overwrites ${overCount} manual/off channel(s)">Apply All (${cleanCount + overCount})</button>` : ''}
           </div>`
      }
    `;
  } catch(e) {
    content.innerHTML = `<p style="color:#f87171">Failed to check: ${escCm(String(e))}</p>
      <div style="display:flex;justify-content:flex-end;margin-top:1rem">
        <button class="btn-sm btn-secondary" onclick="closeApplyAllConfirm()">Close</button>
      </div>`;
  }
}

async function runApplyAll(newOnly = false) {
  const content = document.getElementById('cm-apply-all-content');
  content.innerHTML = '<div class="modal-loading">Applying…</div>';
  try {
    const r = await fetch('/api/gracenote/community-apply-all', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dry_run: false, new_only: newOnly }),
    });
    const d = await r.json();
    content.innerHTML = `
      <h3 style="margin-bottom:0.75rem;font-size:1rem">Done</h3>
      <div style="font-size:0.875rem;color:#cbd5e1;line-height:1.8">
        <div>✅ ${d.applied.length} community IDs applied</div>
        ${d.overwritten.length && !newOnly ? `<div>⚠️ ${d.overwritten.length} manual/off overrides replaced</div>` : ''}
        <div style="color:#475569">${d.already_done} already applied — skipped</div>
      </div>
      <div style="display:flex;justify-content:flex-end;margin-top:1rem">
        <button class="btn-sm btn-secondary" onclick="closeApplyAllConfirm();_cmData=[];openCommunityMap()">Refresh Map</button>
      </div>
    `;
  } catch(e) {
    content.innerHTML = `<p style="color:#f87171">Apply failed: ${escCm(String(e))}</p>
      <div style="display:flex;justify-content:flex-end;margin-top:1rem">
        <button class="btn-sm btn-secondary" onclick="closeApplyAllConfirm()">Close</button>
      </div>`;
  }
}

function _cmGnOnApplied(channelId, stationId) {
  const row = _cmData.find(r => r.channel_id === channelId);
  if (row) { row.current_id = stationId; row.gracenote_mode = 'manual'; row.already_applied = row.community_tmsid === stationId; }
  const cmBtn = document.getElementById(`cm-btn-${channelId}`);
  const cmBadge = cmBtn?.closest('tr')?.querySelector('.cm-badge');
  if (cmBtn && row?.already_applied) {
    cmBtn.textContent = 'Applied'; cmBtn.disabled = true;
    if (cmBadge) { cmBadge.className = 'cm-badge applied'; cmBadge.textContent = 'Applied'; }
  } else if (cmBadge) { cmBadge.className = 'cm-badge manual'; cmBadge.textContent = 'Manual'; }
}

// ── Contribute ───────────────────────────────────────────────────────────────
let _contributeData = [];

function closeContributeModal() {
  document.getElementById('cm-contribute-modal').classList.remove('open');
}

async function openContributeModal() {
  const modal = document.getElementById('cm-contribute-modal');
  const content = document.getElementById('cm-contribute-content');
  content.innerHTML = '<div class="modal-loading">Finding your unique mappings…</div>';
  modal.classList.add('open');
  try {
    const r = await fetch('/api/gracenote/my-contributions');
    const d = await r.json();
    _contributeData = d.results || [];
    renderContributeModal();
  } catch(e) {
    content.innerHTML = `<p style="color:#f87171">Failed to load: ${escCm(String(e))}</p>
      <div style="display:flex;justify-content:flex-end;margin-top:1rem">
        <button class="btn-sm btn-secondary" onclick="closeContributeModal()">Close</button>
      </div>`;
  }
}

function renderContributeModal() {
  const content = document.getElementById('cm-contribute-content');
  const rows = _contributeData;

  if (!rows.length) {
    content.innerHTML = `
      <h3 style="margin-bottom:0.5rem;font-size:1rem">Contribute to Community Map</h3>
      <p style="color:#64748b;font-size:0.875rem;margin-bottom:0.75rem">All your mapped channels are already in the community map — nothing new to contribute yet.</p>
      <div style="background:#0f172a;border:1px solid #334155;border-radius:8px;padding:0.9rem;margin-bottom:1rem">
        <div style="color:#e2e8f0;font-size:0.875rem;margin-bottom:0.4rem">Want to help fill in the gaps?</div>
        <div style="color:#94a3b8;font-size:0.82rem;margin-bottom:0.75rem">Browse channels that don't have a Gracenote ID yet, assign some, then come back here to submit your new mappings to the community.</div>
        <a href="/admin/channels?gracenote=missing" onclick="closeContributeModal()" class="btn-sm" style="background:#1d4ed8;color:#fff;text-decoration:none;display:inline-block">Browse Unmapped Channels →</a>
      </div>
      <div style="display:flex;justify-content:flex-end">
        <button class="btn-sm btn-secondary" onclick="closeContributeModal()">Close</button>
      </div>`;
    return;
  }

  const newCount    = rows.filter(r => !r.in_community).length;
  const updateCount = rows.filter(r => r.in_community).length;

  content.innerHTML = `
    <h3 style="margin-bottom:0.25rem;font-size:1rem">Contribute to Community Map</h3>
    <p style="color:#64748b;font-size:0.82rem;margin-bottom:0.75rem">
      You have <strong style="color:#e2e8f0">${rows.length}</strong> mapping(s) not in the community map
      ${newCount ? `— <span style="color:#86efac">${newCount} new</span>` : ''}
      ${updateCount ? `<span style="color:#fbbf24">${newCount ? ', ' : '— '}${updateCount} with a different ID than community</span>` : ''}.
      Select which ones to submit.
    </p>
    <div style="display:flex;gap:0.5rem;margin-bottom:0.5rem">
      <button class="btn-sm btn-secondary" onclick="_contributeSelectAll(true)" style="font-size:0.75rem;padding:0.2rem 0.5rem">All</button>
      <button class="btn-sm btn-secondary" onclick="_contributeSelectAll(false)" style="font-size:0.75rem;padding:0.2rem 0.5rem">None</button>
      <span id="contrib-sel-count" style="color:#64748b;font-size:0.8rem;align-self:center"></span>
    </div>
    <div style="max-height:280px;overflow-y:auto;border:1px solid #334155;border-radius:6px;margin-bottom:0.75rem">
      <table style="width:100%;border-collapse:collapse;font-size:0.82rem">
        <thead><tr style="background:#0f172a">
          <th style="padding:0.35rem 0.5rem;text-align:left;color:#94a3b8;font-weight:600;width:1.5rem"></th>
          <th style="padding:0.35rem 0.5rem;text-align:left;color:#94a3b8;font-weight:600">Channel</th>
          <th style="padding:0.35rem 0.5rem;text-align:left;color:#94a3b8;font-weight:600">Source</th>
          <th style="padding:0.35rem 0.5rem;text-align:left;color:#94a3b8;font-weight:600">Your ID</th>
          <th style="padding:0.35rem 0.5rem;text-align:left;color:#94a3b8;font-weight:600">Type</th>
        </tr></thead>
        <tbody id="contrib-tbody">
          ${rows.map(row => `
            <tr style="border-bottom:1px solid #1e293b">
              <td style="padding:0.3rem 0.5rem"><input type="checkbox" class="contrib-cb" data-id="${row.channel_id}" checked onchange="_updateContribCount()"></td>
              <td style="padding:0.3rem 0.5rem;color:#e2e8f0">${escCm(row.channel_name)}</td>
              <td style="padding:0.3rem 0.5rem;color:#94a3b8">${escCm(row.source_name)}</td>
              <td style="padding:0.3rem 0.5rem"><code style="font-size:0.78rem;color:#7dd3fc">${escCm(row.tmsid)}</code></td>
              <td style="padding:0.3rem 0.5rem">
                ${row.in_community
                  ? `<span style="color:#fbbf24;font-size:0.75rem">differs</span>`
                  : `<span style="color:#86efac;font-size:0.75rem">new</span>`}
              </td>
            </tr>`).join('')}
        </tbody>
      </table>
    </div>
    <div id="contrib-result"></div>
    <div style="display:flex;gap:0.5rem;justify-content:flex-end;margin-top:0.5rem">
      <button class="btn-sm btn-secondary" onclick="closeContributeModal()">Cancel</button>
      <button class="btn-sm" id="contrib-submit-btn" onclick="submitContributions()" style="background:#1d4ed8;color:#fff">Submit Selected</button>
    </div>`;
  _updateContribCount();
  _applyContribCooldown();
}

function _contributeSelectAll(checked) {
  document.querySelectorAll('.contrib-cb').forEach(cb => cb.checked = checked);
  _updateContribCount();
}

function _updateContribCount() {
  const n = document.querySelectorAll('.contrib-cb:checked').length;
  const el = document.getElementById('contrib-sel-count');
  if (el) el.textContent = `${n} selected`;
  const btn = document.getElementById('contrib-submit-btn');
  if (btn) btn.disabled = n === 0;
}

const _CONTRIB_COOLDOWN_MS = 24 * 60 * 60 * 1000;
const _CONTRIB_LS_KEY = 'fc_contrib_last_at';

function _contribCooldownRemaining() {
  const last = parseInt(localStorage.getItem(_CONTRIB_LS_KEY) || '0', 10);
  if (!last) return 0;
  return Math.max(0, last + _CONTRIB_COOLDOWN_MS - Date.now());
}

function _contribCooldownLabel(ms) {
  const h = Math.floor(ms / 3600000);
  const m = Math.ceil((ms % 3600000) / 60000);
  if (h > 0) return `${h}h ${m}m`;
  return `${m}m`;
}

function _applyContribCooldown() {
  const remaining = _contribCooldownRemaining();
  const btn = document.getElementById('contrib-submit-btn');
  const result = document.getElementById('contrib-result');
  if (!btn) return;
  if (remaining > 0) {
    btn.disabled = true;
    btn.textContent = 'Submit Selected';
    btn.title = `Cooldown active — resubmit available in ${_contribCooldownLabel(remaining)}`;
    if (result) result.innerHTML = `<div style="color:#94a3b8;font-size:0.82rem;margin-bottom:0.5rem">⏳ You already submitted recently. Next submission available in <strong>${_contribCooldownLabel(remaining)}</strong>.</div>`;
  }
}

async function submitContributions() {
  const remaining = _contribCooldownRemaining();
  if (remaining > 0) {
    const result = document.getElementById('contrib-result');
    if (result) result.innerHTML = `<div style="color:#fbbf24;font-size:0.875rem;margin-bottom:0.5rem">⏳ Please wait ${_contribCooldownLabel(remaining)} before submitting again.</div>`;
    return;
  }
  const ids = [...document.querySelectorAll('.contrib-cb:checked')].map(cb => parseInt(cb.dataset.id));
  if (!ids.length) return;
  const btn = document.getElementById('contrib-submit-btn');
  btn.disabled = true; btn.textContent = 'Submitting…';
  const result = document.getElementById('contrib-result');
  try {
    const r = await fetch('/api/gracenote/submit-contributions', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({channel_ids: ids}),
    });
    const d = await r.json();
    if (r.status === 429) {
      result.innerHTML = `<div style="color:#fbbf24;font-size:0.875rem;margin-bottom:0.5rem">⏳ ${escCm(d.message)}</div>`;
      btn.disabled = false; btn.textContent = 'Submit Selected';
      return;
    }
    if (d.ok) {
      localStorage.setItem(_CONTRIB_LS_KEY, String(Date.now()));
      let msg = `<div style="color:#86efac;font-size:0.875rem;margin-bottom:0.5rem">✅ ${d.submitted} mapping(s) submitted — thank you!</div>`;
      if (d.failed > 0) {
        msg += `<div style="color:#f87171;font-size:0.82rem;margin-bottom:0.5rem">⚠ ${d.failed} failed to deliver: ${escCm((d.failed_names || []).join(', '))}</div>`;
      }
      result.innerHTML = msg;
      btn.textContent = d.failed > 0 ? 'Partial' : 'Done';
      btn.style.background = d.failed > 0 ? '#92400e' : '#166534';
    } else {
      result.innerHTML = `<div style="color:#f87171;font-size:0.875rem;margin-bottom:0.5rem">❌ ${escCm(d.message)}</div>`;
      btn.disabled = false; btn.textContent = 'Submit Selected';
    }
  } catch(e) {
    result.innerHTML = `<div style="color:#f87171;font-size:0.875rem;margin-bottom:0.5rem">❌ ${escCm(String(e))}</div>`;
    btn.disabled = false; btn.textContent = 'Submit Selected';
  }
}

document.addEventListener('keydown', e => {
  if (e.key === 'Escape') {
    if (typeof closeGracenoteSuggestModal === 'function') closeGracenoteSuggestModal();
    closeCommunityMap();
  }
});
