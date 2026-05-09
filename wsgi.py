from gevent import monkey

# Gunicorn gevent workers warn if ssl is imported before monkey-patching.
# Keep preload enabled, but patch at the earliest possible import point.
monkey.patch_all()

import sys as _sys
import asyncio as _asyncio

def _install_gevent_asyncio_filter():
    # sync_playwright() creates a native OS thread with its own asyncio event
    # loop to bridge the sync API.  When that loop closes, asyncio calls
    # call_exception_handler to log cleanup errors.  gevent has monkey-patched
    # threading.current_thread(), whose _DummyThread.__init__ asserts that
    # getcurrent() is not None — but it IS None in a native thread, so the
    # assert fires.  The error is harmless noise; suppress it precisely.
    _orig = _sys.unraisablehook
    def _hook(u):
        if (isinstance(u.exc_value, AssertionError)
                and isinstance(getattr(u.object, '__self__', None), _asyncio.BaseEventLoop)
                and getattr(getattr(u.object, '__func__', None), '__name__', '') == 'call_exception_handler'):
            return
        _orig(u)
    _sys.unraisablehook = _hook

_install_gevent_asyncio_filter()

from app import create_app

app = create_app()
