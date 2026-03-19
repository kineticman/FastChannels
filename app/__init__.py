import os
import time
import logging
from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix
from .extensions import db
from .config import Config, VERSION
from . import logfile
from .schema import ensure_runtime_schema
from .version_check import get_version_status


_memlog = logging.getLogger('app.worker_mem')


def _read_rss_bytes() -> int | None:
    try:
        with open('/proc/self/status', 'r', encoding='utf-8') as fp:
            for line in fp:
                if line.startswith('VmRSS:'):
                    return int(line.split()[1]) * 1024
    except OSError:
        return None
    return None


def _fmt_mb(value: int | None) -> str:
    if value is None:
        return '?'
    return f'{value / (1024 * 1024):.1f}MB'

def _ensure_sqlite_parent_dir(database_uri: str | None) -> None:
    if not database_uri or not database_uri.startswith("sqlite:"):
        return

    path = database_uri[len("sqlite:"):]
    if path.startswith("////"):
        fs_path = path[3:]
    elif path.startswith("///"):
        fs_path = path[2:]
    else:
        return

    parent = os.path.dirname(fs_path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def create_app(config_class=Config):
    logfile.setup()
    app = Flask(__name__)
    # Trust X-Forwarded-Proto/Host from reverse proxies (Nginx, Traefik, Caddy, etc.)
    # so that request.host_url reflects the public https:// scheme instead of the
    # internal http:// connection, keeping logo proxy URLs scheme-correct in M3U output.
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
    app.config.from_object(config_class)
    _ensure_sqlite_parent_dir(app.config.get("SQLALCHEMY_DATABASE_URI"))

    mem_trace_enabled = (os.environ.get('FASTCHANNELS_WORKER_TRACE') or '').strip() == '1'
    mem_trace_delta_mb = int((os.environ.get('FASTCHANNELS_WORKER_TRACE_DELTA_MB') or '16').strip() or '16')
    mem_trace_rss_mb = int((os.environ.get('FASTCHANNELS_WORKER_TRACE_RSS_MB') or '200').strip() or '200')
    mem_trace_slow_ms = int((os.environ.get('FASTCHANNELS_WORKER_TRACE_SLOW_MS') or '1500').strip() or '1500')

    @app.context_processor
    def inject_version():
        return {
            'app_version': VERSION,
            'update_status': get_version_status(
                VERSION,
                enabled=app.config.get('VERSION_CHECK_ENABLED', True),
                repo=app.config.get('VERSION_CHECK_REPO', 'kineticman/FastChannels'),
                ttl_hours=app.config.get('VERSION_CHECK_TTL_HOURS', 12),
            ),
        }

    @app.template_filter('localtime')
    def localtime_filter(dt):
        """Format a UTC datetime as local time (respects TZ env var)."""
        if dt is None:
            return 'Never'
        return dt.astimezone().strftime('%Y-%m-%d %H:%M %Z')

    db.init_app(app)
    with app.app_context():
        from sqlalchemy import event
        import sqlite3 as _sqlite3

        @event.listens_for(db.engine, "connect")
        def _set_sqlite_pragmas(dbapi_conn, _):
            if isinstance(dbapi_conn, _sqlite3.Connection):
                dbapi_conn.execute("PRAGMA journal_mode=WAL")
                dbapi_conn.execute("PRAGMA busy_timeout=30000")

        ensure_runtime_schema()

    if mem_trace_enabled:
        from flask import g, request

        @app.before_request
        def _trace_request_start():
            g._trace_pid = os.getpid()
            g._trace_started_at = time.perf_counter()
            g._trace_rss_before = _read_rss_bytes()
            _memlog.info(
                '[worker-trace] pid=%s start %s %s rss=%s',
                g._trace_pid,
                request.method,
                request.full_path if request.query_string else request.path,
                _fmt_mb(g._trace_rss_before),
            )

        @app.after_request
        def _trace_request_end(response):
            g._trace_status_code = response.status_code
            return response

        @app.teardown_request
        def _trace_request_teardown(exc):
            started_at = getattr(g, '_trace_started_at', None)
            rss_before = getattr(g, '_trace_rss_before', None)
            pid = getattr(g, '_trace_pid', os.getpid())
            rss_after = _read_rss_bytes()
            elapsed_ms = ((time.perf_counter() - started_at) * 1000) if started_at else 0
            delta = (rss_after - rss_before) if (rss_before is not None and rss_after is not None) else None
            status_code = getattr(g, '_trace_status_code', 500 if exc else None)
            should_log = (
                mem_trace_slow_ms <= 0
                or
                (status_code is not None and status_code >= 500)
                or elapsed_ms >= mem_trace_slow_ms
                or (delta is not None and delta >= mem_trace_delta_mb * 1024 * 1024)
                or (rss_after is not None and rss_after >= mem_trace_rss_mb * 1024 * 1024)
            )
            if should_log:
                _memlog.info(
                    '[worker-trace] pid=%s done %s %s status=%s dur=%.0fms rss=%s->%s delta=%s',
                    pid,
                    request.method,
                    request.full_path if request.query_string else request.path,
                    status_code,
                    elapsed_ms,
                    _fmt_mb(rss_before),
                    _fmt_mb(rss_after),
                    _fmt_mb(delta) if delta is not None else '?',
                )

    from .routes.output import output_bp
    from .routes.api import api_bp
    from .routes.feeds_api import feeds_api_bp
    from .routes.admin import admin_bp
    from .routes.play import play_bp
    from .routes.images import images_bp

    app.register_blueprint(output_bp)
    app.register_blueprint(api_bp, url_prefix='/api')
    app.register_blueprint(feeds_api_bp, url_prefix='/api/feeds')
    app.register_blueprint(admin_bp, url_prefix='/admin')
    app.register_blueprint(play_bp)   # /play/<source>/<id>.m3u8
    app.register_blueprint(images_bp) # /images/proxy

    from flask import redirect, url_for

    @app.route('/')
    def root():
        return redirect(url_for('admin.dashboard'))

    return app
