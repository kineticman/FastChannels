async function setLiteModePreference(enabled) {
  const response = await fetch('/api/settings/lite-mode', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({enabled}),
  });
  if (!response.ok) throw new Error('Could not save interface preference.');
  return response.json();
}
