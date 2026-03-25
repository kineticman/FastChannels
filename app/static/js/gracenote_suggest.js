/**
 * gracenote_suggest.js
 *
 * Shared Gracenote Suggestions popup logic used by channels, settings,
 * and help admin pages.
 *
 * Requires: a gs-modal overlay element and an escHtml() function defined
 * on the page (or the bundled one below is used as default).
 *
 * Public API:
 *   openGracenoteSuggestModal(channelId, { onApplied })
 *   closeGracenoteSuggestModal()
 */

(function (global) {

  function escHtml(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g,  '&lt;')
      .replace(/>/g,  '&gt;')
      .replace(/"/g,  '&quot;');
  }

  function fmtTime(iso) {
    if (!iso) return '';
    try {
      return new Date(iso).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
    } catch { return ''; }
  }

  function fmtEpisode(p) {
    const parts = [];
    if (p?.episode_title) parts.push(escHtml(p.episode_title));
    if (p?.season && p?.episode)
      parts.push(`S${String(p.season).padStart(2,'0')}E${String(p.episode).padStart(2,'0')}`);
    else if (p?.season)
      parts.push(`Season ${p.season}`);
    return parts.length ? `<div class="gs-channel-now-sub">${parts.join(' · ')}</div>` : '';
  }

  // ── Render now-playing slot into a gs-now-playing placeholder ───────────────

  function renderNowPlayingInto(el, data) {
    if (!el) return;
    if (!data.found || data.error === 'not_in_index') {
      el.innerHTML = '<div class="gs-now-missing">Not in FAST guide index</div>';
      return;
    }
    if (!data.now && !data.next) {
      el.innerHTML = '<div class="gs-now-missing">No guide data available</div>';
      return;
    }
    const nowTime  = (data.now?.start && data.now?.end)
      ? `${fmtTime(data.now.start)} – ${fmtTime(data.now.end)}` : '';
    const nextTime = data.next?.start ? `Starts ${fmtTime(data.next.start)}` : '';
    const nowSub   = data.now?.subtitle
      ? `<div class="gs-now-subtitle">${escHtml(data.now.subtitle)}</div>` : '';
    const nextSub  = data.next?.subtitle
      ? `<div class="gs-now-subtitle">${escHtml(data.next.subtitle)}</div>` : '';
    el.innerHTML = `
      <div class="gs-now-slot">
        <div class="gs-now-label now">Now</div>
        <div class="gs-now-title${data.now?.title ? '' : ' dim'}">${escHtml(data.now?.title || '—')}</div>
        ${nowSub}
        <div class="gs-now-time">${escHtml(nowTime)}</div>
      </div>
      <div class="gs-now-slot">
        <div class="gs-now-label">Next</div>
        <div class="gs-now-title dim">${escHtml(data.next?.title || '—')}</div>
        ${nextSub}
        <div class="gs-now-time">${escHtml(nextTime)}</div>
      </div>`;
  }

  // ── Render full suggestions HTML ─────────────────────────────────────────────

  function renderGracenoteSuggestions(ch, payload, previewData) {
    const results    = Array.isArray(payload?.results) ? payload.results : [];
    const dvrMissing = !!payload?.dvr_missing;
    const currentId  = String(ch?.gracenote_id || '').trim();
    const gnSource   = ch?.gracenote_source || null;
    const csvSuggestion = ch?.csv_suggestion || null;

    // Channel's own now/next from scraper preview
    let channelNowHtml = '';
    const cur = previewData?.current_program;
    const nxt = previewData?.next_program;
    if (cur?.title || nxt?.title) {
      const curTime = (cur?.start_time && cur?.end_time)
        ? `${fmtTime(cur.start_time)} – ${fmtTime(cur.end_time)}` : '';
      const nxtTime = nxt?.start_time ? `Starts ${fmtTime(nxt.start_time)}` : '';
      channelNowHtml = `
        <div class="gs-channel-now">
          <div class="gs-channel-now-slot">
            <div class="gs-channel-now-label">Now Playing</div>
            <div class="gs-channel-now-title${cur?.title ? '' : ' dim'}">${escHtml(cur?.title || '—')}</div>
            ${fmtEpisode(cur)}
            <div class="gs-channel-now-time">${escHtml(curTime)}</div>
          </div>
          <div class="gs-channel-now-slot">
            <div class="gs-channel-now-label next">Up Next</div>
            <div class="gs-channel-now-title dim">${escHtml(nxt?.title || '—')}</div>
            ${fmtEpisode(nxt)}
            <div class="gs-channel-now-time">${escHtml(nxtTime)}</div>
          </div>
        </div>`;
    }

    // Current ID badge
    let currentLine = '';
    if (currentId) {
      const badgeMap = {
        manual: ['manual', 'manual'],
        native: ['native', 'source-verified'],
        csv:    ['csv',    'community map'],
      };
      const [cls, label] = badgeMap[gnSource] || [];
      const badge = cls
        ? `<span class="gs-source-badge ${escHtml(cls)}">${escHtml(label)}</span>` : '';
      currentLine = `<div class="gs-current">Current Gracenote ID: <strong>${escHtml(currentId)}</strong>${badge}</div>`;
    }

    // Community map section
    let communitySection = '';
    if (csvSuggestion?.tmsid) {
      const csvId = String(csvSuggestion.tmsid);
      const csvNotes = csvSuggestion.notes ? ` · ${escHtml(csvSuggestion.notes)}` : '';
      const alreadySet = currentId === csvId;
      communitySection = `
        <div class="gs-community-section">
          <div class="gs-community-label">Community Map</div>
          <div class="gs-community-item">
            <div class="gs-suggest-main">
              <div class="gs-suggest-station">${escHtml(csvId)}</div>
              <div class="gs-suggest-detail">Mapped in gracenote_map.csv${csvNotes}</div>
              <div class="gs-suggest-reasons">Not independently verified — confirm against Channels DVR or spot-check the schedule.</div>
              <div class="gs-now-playing" id="gs-now-csv-${escHtml(csvId)}">
                <div class="gs-now-loading">Loading guide data…</div>
              </div>
            </div>
            <div class="gs-suggest-actions">
              <span class="gs-source-badge csv">community map</span>
              <button class="btn-apply-gn"
                onclick="gsApplySuggestion(${Number(ch.id)}, '${escHtml(csvId)}', this)"
                ${alreadySet ? 'disabled' : ''}
              >${alreadySet ? 'Applied' : 'Apply'}</button>
            </div>
          </div>
        </div>`;
    }

    // DVR results section
    let dvrSection = '';
    if (dvrMissing) {
      dvrSection = `
        <div class="gs-dvr-label">Channels DVR</div>
        <div class="gs-no-dvr">Channels DVR URL is not configured — station search unavailable. Set it in Settings to enable ranked suggestions.</div>`;
    } else if (!results.length) {
      dvrSection = `
        <div class="gs-dvr-label">Channels DVR</div>
        <div class="gs-suggest-empty">No candidates came back from Channels DVR for this channel name.</div>`;
    } else {
      const items = results.map((item, idx) => {
        const reasons = Array.isArray(item.reasons) && item.reasons.length
          ? `<div class="gs-suggest-reasons">${escHtml(item.reasons.join(' · '))}</div>` : '';
        const bits = [
          item.affiliate || '', item.type || '', item.video || '',
          item.primary_language || '', item.call_sign || '',
        ].filter(Boolean);
        const stationId  = String(item.station_id || '');
        const alreadySet = currentId && currentId === stationId;
        return `
          <div class="gs-suggest-item">
            <div class="gs-suggest-main">
              <div class="gs-suggest-title">${escHtml(item.name || 'Unknown')}</div>
              <div class="gs-suggest-station">${escHtml(stationId)}</div>
              <div class="gs-suggest-detail">${escHtml(bits.join(' · ') || 'No extra station details')}</div>
              ${reasons}
              ${stationId ? `<div class="gs-now-playing" id="gs-now-dvr-${idx}-${escHtml(stationId)}"><div class="gs-now-loading">Loading guide data…</div></div>` : ''}
            </div>
            <div class="gs-suggest-actions">
              <span class="preview-conf ${escHtml(item.confidence || 'weak')}">${escHtml(item.confidence || 'weak')}</span>
              <button class="btn-apply-gn"
                onclick="gsApplySuggestion(${Number(ch.id)}, '${escHtml(stationId)}', this)"
                ${alreadySet ? 'disabled' : ''}
              >${alreadySet ? 'Applied' : 'Apply'}</button>
            </div>
          </div>`;
      }).join('');
      dvrSection = `
        <div class="gs-dvr-label">Channels DVR</div>
        <div class="gs-suggest-meta">Results come from your Channels DVR station search and are ranked locally, so the top row should usually be the useful one.</div>
        <div class="gs-suggest-list">${items}</div>`;
    }

    return `
      <div class="gs-subsection">
        ${channelNowHtml}
        ${currentLine}
        ${communitySection}
        ${dvrSection}
      </div>`;
  }

  // ── Open / close modal ────────────────────────────────────────────────────────

  async function openGracenoteSuggestModal(channelId, opts) {
    const modal   = document.getElementById('gs-suggest-modal');
    const content = document.getElementById('gs-suggest-modal-content');
    content.innerHTML = '<div class="modal-loading">Loading suggestions…</div>';
    modal.classList.add('open');

    try {
      const [suggestResp, previewResp] = await Promise.all([
        fetch(`/api/channels/${channelId}/gracenote-suggestions?limit=5`),
        fetch(`/api/channels/${channelId}/preview`),
      ]);
      const data = await suggestResp.json();
      if (!suggestResp.ok) throw new Error(data.error || `HTTP ${suggestResp.status}`);
      const previewData = previewResp.ok ? await previewResp.json() : null;
      const ch = data.channel || {};

      content.innerHTML = `
        <div class="gs-header">
          <div class="gs-title">Gracenote Suggestions</div>
          <div class="gs-meta">
            ${escHtml(ch.name || '')}
            ${ch.source_name ? ` · ${escHtml(ch.source_name)}` : ''}
            ${ch.category    ? ` · ${escHtml(ch.category)}`    : ''}
            ${ch.language    ? ` · ${escHtml(ch.language)}`    : ''}
          </div>
        </div>
        ${renderGracenoteSuggestions(ch, data, previewData)}
        <div class="preview-actions" style="margin-top:1rem;margin-bottom:0">
          <button class="btn-cancel-modal" onclick="closeGracenoteSuggestModal()">Close</button>
        </div>`;

      // Fire tvtv now-playing lookups — use unique element IDs to avoid duplicate-ID conflicts
      // when the same station appears in both the community map and DVR results.
      const lookups = [
        ...(data.channel?.csv_suggestion?.tmsid
          ? [{ sid: String(data.channel.csv_suggestion.tmsid), eid: `gs-now-csv-${data.channel.csv_suggestion.tmsid}` }]
          : []),
        ...(Array.isArray(data.results)
          ? data.results
              .map((r, i) => ({ sid: String(r.station_id || ''), eid: `gs-now-dvr-${i}-${r.station_id}` }))
              .filter(x => x.sid)
          : []),
      ];
      lookups.forEach(async ({ sid, eid }) => {
        try {
          const r = await fetch(`/api/stations/${encodeURIComponent(sid)}/now-playing`);
          const d = await r.json();
          const el = document.getElementById(eid);
          if (el) renderNowPlayingInto(el, d);
        } catch {
          const el = document.getElementById(eid);
          if (el) el.innerHTML = '<div class="gs-now-missing">Guide lookup failed</div>';
        }
      });

      if (opts?.onApplied) { global._gsOnApplied = opts.onApplied; }

    } catch (err) {
      content.innerHTML = `
        <div class="gs-header"><div class="gs-title">Gracenote Suggestions</div></div>
        <div class="gs-suggest-error">${escHtml(String(err.message || err))}</div>
        <div class="preview-actions" style="margin-top:1rem;margin-bottom:0">
          <button class="btn-cancel-modal" onclick="closeGracenoteSuggestModal()">Close</button>
        </div>`;
    }
  }

  function closeGracenoteSuggestModal() {
    document.getElementById('gs-suggest-modal')?.classList.remove('open');
    global._gsOnApplied = null;
  }

  // ── Apply a suggestion ────────────────────────────────────────────────────────

  async function gsApplySuggestion(channelId, stationId, btn) {
    if (!stationId) return;
    const orig = btn.textContent;
    btn.disabled = true;
    btn.textContent = 'Saving…';
    try {
      const r = await fetch(`/api/channels/${channelId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ gracenote_id: stationId, gracenote_mode: 'manual' }),
      });
      const data = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);
      btn.textContent = 'Applied';
      // Update other buttons in the modal
      const content = document.getElementById('gs-suggest-modal-content');
      content?.querySelector('.gs-current') && (
        content.querySelector('.gs-current').innerHTML =
          `Current Gracenote ID: <strong>${escHtml(stationId)}</strong><span class="gs-source-badge manual">manual</span>`
      );
      content?.querySelectorAll('.btn-apply-gn').forEach(el => {
        if (el !== btn) { el.textContent = 'Apply'; el.disabled = false; }
      });
      if (global._gsOnApplied) global._gsOnApplied(channelId, stationId);
    } catch (e) {
      btn.disabled = false;
      btn.textContent = orig;
      alert(`Failed to apply: ${e.message}`);
    }
  }

  // ── Exports ───────────────────────────────────────────────────────────────────

  global.openGracenoteSuggestModal  = openGracenoteSuggestModal;
  global.closeGracenoteSuggestModal = closeGracenoteSuggestModal;
  global.gsApplySuggestion          = gsApplySuggestion;
  global.renderNowPlayingInto       = renderNowPlayingInto; // used by channels.html inline fetch loop

})(window);
