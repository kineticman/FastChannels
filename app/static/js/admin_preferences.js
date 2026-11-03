async function setLiteModePreference(enabled) {
  const response = await fetch('/api/settings/lite-mode', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({enabled}),
  });
  if (!response.ok) throw new Error('Could not save interface preference.');
  return response.json();
}

// Header Mode picker (_nav.html): save and reload so every page re-renders in the new mode.
async function onModePickerChange(select) {
  const wantLite = select.value === 'lite';
  select.disabled = true;
  try {
    await setLiteModePreference(wantLite);
    window.location.reload();
  } catch (error) {
    select.value = wantLite ? 'advanced' : 'lite';
    select.disabled = false;
    alert('Could not save the interface mode. Please try again.');
  }
}

// Lite-mode notice (_lite_notice.html): dismissed per browser.
const LITE_NOTICE_KEY = 'fc.liteNoticeDismissed';
function initLiteNotice() {
  const box = document.getElementById('lite-notice');
  if (!box) return;
  let dismissed = false;
  try { dismissed = localStorage.getItem(LITE_NOTICE_KEY) === '1'; } catch (e) { /* storage blocked */ }
  if (!dismissed) box.hidden = false;
}
function dismissLiteNotice() {
  const box = document.getElementById('lite-notice');
  if (box) box.hidden = true;
  try { localStorage.setItem(LITE_NOTICE_KEY, '1'); } catch (e) { /* storage blocked */ }
}
async function liteNoticeShowAll(button) {
  button.disabled = true;
  try {
    await setLiteModePreference(false);
    window.location.reload();
  } catch (error) {
    button.disabled = false;
    alert('Could not save the interface mode. Please try again.');
  }
}
