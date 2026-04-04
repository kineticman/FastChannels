(function () {
  const STORAGE_KEY = "fastchannels-admin-theme";
  const DEFAULT_THEME = "dark";
  const THEMES = new Set(["dark", "light", "fun"]);

  function resolveTheme(theme) {
    return THEMES.has(theme) ? theme : DEFAULT_THEME;
  }

  function preferredTheme() {
    try {
      const saved = window.localStorage.getItem(STORAGE_KEY);
      if (saved && THEMES.has(saved)) {
        return saved;
      }
    } catch (err) {
      // Ignore storage access failures and fall back to the default theme.
    }
    return DEFAULT_THEME;
  }

  function applyTheme(theme) {
    const resolved = resolveTheme(theme);
    document.documentElement.setAttribute("data-theme", resolved);
    document.querySelectorAll("[data-admin-theme-select]").forEach((select) => {
      if (select.value !== resolved) {
        select.value = resolved;
      }
    });
    return resolved;
  }

  window.FastChannelsAdminTheme = {
    applyTheme,
    preferredTheme,
  };

  document.addEventListener("DOMContentLoaded", function () {
    const current = applyTheme(preferredTheme());
    document.querySelectorAll("[data-admin-theme-select]").forEach((select) => {
      select.value = current;
      select.addEventListener("change", function (event) {
        const theme = applyTheme(event.target.value);
        try {
          window.localStorage.setItem(STORAGE_KEY, theme);
        } catch (err) {
          // Ignore storage access failures.
        }
      });
    });
  });
})();
