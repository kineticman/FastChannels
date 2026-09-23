"""Shared "debug logging" toggle for any module that wants extra opt-in
diagnostic logging without spamming the normal INFO-level logs.

The root logger is hard-capped at INFO (app/logfile.py's setup() clamps it
there unconditionally), so a bare logger.debug() call anywhere in this app
is silently dropped before it ever reaches a handler — it isn't that the
file handler filters it out, the LOGGER's own effective level gates whether
the log record gets created at all. The established pattern (originally
stream_detector.py's FC_YTDLP_VERBOSE) is an opt-in flag, logged at INFO
under a distinct tag when the flag is on.

This module generalizes that pattern with three independent ways to enable
it, checked together — any ONE is sufficient:
  - A module's own env var (e.g. FC_SPECTRUM_DEBUG), for a power user who
    wants just THAT module's noise without turning everything else on too.
  - GLOBAL_ENV_VAR (FC_DEBUG) — a blanket override that enables every
    module using this helper at once, without each caller needing to
    remember to check for it individually; baked into env_flag_enabled
    itself, not something callers list.
  - AppSettings.debug_logging_enabled, a single DB-backed toggle editable
    from Settings (System Stats card) — the accessible path for anyone who
    can't touch the container's environment, which is most real users. This
    is what makes "turn on debug logging and try again" an actually usable
    ask when helping someone troubleshoot remotely.

Only ONE global DB toggle exists (not per-feature) — deliberately matched
by FC_DEBUG being a blanket override too, so the env-var side and the
DB-backed side have the same two-tier shape (a global "everything" switch,
plus optional per-module scoping only the env-var side bothers to offer).
Each module still decides for itself what "debug" means and what to log —
this just answers the single yes/no question of whether that logging is
currently wanted anywhere.
"""
from __future__ import annotations

import os

GLOBAL_ENV_VAR = 'FC_DEBUG'


def env_flag_enabled(*env_names: str) -> bool:
    """Safe to call from anywhere, including outside a Flask app context
    (e.g. from inside a Camoufox browser session after its caller has
    popped its own context) — this never touches the DB. Always also checks
    GLOBAL_ENV_VAR, on top of whatever module-specific name(s) are passed."""
    for name in (*env_names, GLOBAL_ENV_VAR):
        if (os.environ.get(name) or '').strip().lower() in ('1', 'true', 'yes', 'on'):
            return True
    return False


def settings_flag_enabled() -> bool:
    """Reads AppSettings.debug_logging_enabled — requires an ACTIVE Flask
    app context (it's a DB query). Callers that run partly outside one
    (run_spectrum_signin and friends, which pop their app context before
    launching Camoufox) must read this once up front, while the context is
    still live, and cache the result locally rather than calling this from
    inside the browser session. Fails closed (False) on any error — a
    debug-logging toggle should never be able to break a real sign-in
    attempt."""
    try:
        from app.models import AppSettings
        return bool(AppSettings.get().debug_logging_enabled)
    except Exception:  # noqa: BLE001
        return False


def debug_logging_enabled(*env_names: str, settings_checked: bool | None = None) -> bool:
    """Convenience combinator: True if EITHER the env var(s) are set OR the
    DB setting is on. Pass settings_checked=<cached bool> when calling from
    a context where settings_flag_enabled() itself isn't safe to call
    (see its docstring) — omit it (or pass None) to check the DB directly,
    which is only safe within an active app context."""
    if env_flag_enabled(*env_names):
        return True
    if settings_checked is not None:
        return settings_checked
    return settings_flag_enabled()
