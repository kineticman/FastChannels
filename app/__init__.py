import os
import logging
import secrets
from flask import Flask, g, jsonify, request
from werkzeug.exceptions import HTTPException
from werkzeug.middleware.proxy_fix import ProxyFix
from .extensions import db
from .config import Config, VERSION
from . import logfile
from .schema import ensure_runtime_schema
from .models import AppSettings, Channel
from .timezone_utils import format_datetime, write_timezone_cache
from .version_check import get_version_status

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

    @app.before_request
    def assign_request_id():
        incoming = (request.headers.get('X-Request-ID') or '').strip()
        g.request_id = incoming[:80] if incoming else secrets.token_hex(8)

    @app.after_request
    def attach_request_diagnostics(response):
        request_id = getattr(g, 'request_id', None)
        if request_id:
            response.headers['X-FastChannels-Request-ID'] = request_id
        failure_stage = getattr(g, 'failure_stage', None)
        if failure_stage and response.status_code >= 500:
            response.headers['X-FastChannels-Failure-Stage'] = failure_stage
        return response

    @app.errorhandler(Exception)
    def handle_unexpected_error(exc):
        if isinstance(exc, HTTPException):
            return exc
        path = request.path or ''
        if '/license' in path or '/certificate' in path:
            stage = 'license'
        elif '/watch/' in path:
            stage = 'watch-page'
        elif '/dash.mpd' in path or path.endswith('.m3u8'):
            stage = 'manifest'
        elif '/play' in path:
            stage = 'resolve'
        else:
            stage = 'server'
        g.failure_stage = stage
        request_id = getattr(g, 'request_id', '-')
        logging.getLogger(__name__).exception(
            '[http-error] request_id=%s stage=%s method=%s path=%s',
            request_id, stage, request.method, path,
        )
        return jsonify({
            'error': 'Internal server error',
            'request_id': request_id,
            'failure_stage': stage,
            'path': path,
        }), 500

    # Trust X-Forwarded-Proto/Host/Port from reverse proxies (Nginx, Traefik, Caddy, etc.)
    # so that request.host_url reflects the public scheme/host/port instead of the
    # internal connection details. x_port ensures standard ports (80/443) are not
    # appended to generated URLs when the proxy forwards X-Forwarded-Port.
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1, x_port=1)
    app.config.from_object(config_class)
    _ensure_sqlite_parent_dir(app.config.get("SQLALCHEMY_DATABASE_URI"))

    @app.context_processor
    def inject_version():
        settings = AppSettings.get()
        try:
            pending_review_count = (
                Channel.query
                .filter(Channel.review_state == 'pending')
                .count()
            )
        except Exception:
            # Never let a count query break page rendering (e.g. mid-migration).
            pending_review_count = 0
        return {
            'app_version': VERSION,
            'app_timezone_name': settings.effective_timezone_name(),
            'pending_review_count': pending_review_count,
            'update_status': get_version_status(
                VERSION,
                enabled=app.config.get('VERSION_CHECK_ENABLED', True),
                repo=app.config.get('VERSION_CHECK_REPO', 'kineticman/FastChannels'),
                ttl_hours=app.config.get('VERSION_CHECK_TTL_HOURS', 12),
            ),
        }

    @app.template_filter('localtime')
    def localtime_filter(dt):
        """Format a UTC datetime in the user-selected timezone."""
        settings = AppSettings.get()
        return format_datetime(dt, timezone_name=settings.effective_timezone_name())

    db.init_app(app)
    with app.app_context():
        from sqlalchemy import event
        import sqlite3 as _sqlite3

        @event.listens_for(db.engine, "connect")
        def _set_sqlite_pragmas(dbapi_conn, _):
            if isinstance(dbapi_conn, _sqlite3.Connection):
                dbapi_conn.execute("PRAGMA journal_mode=WAL")
                dbapi_conn.execute("PRAGMA busy_timeout=30000")
                dbapi_conn.execute("PRAGMA foreign_keys=ON")

        # Fresh installs need the base tables before any startup path queries
        # AppSettings (for timezone cache, template globals, etc.).
        db.create_all()
        # Skip schema migration if the entrypoint already ran it (FC_SCHEMA_READY=1).
        # This prevents write-write lock contention when the worker and gunicorn
        # both call create_app() simultaneously at container startup — and, since
        # every request-time `import app.worker` re-triggers this module's
        # `create_app()` too, it must stay a no-op write on the hot path or every
        # such call takes a DB write lock and can stall concurrent requests.
        import os as _os
        if not _os.environ.get('FC_SCHEMA_READY'):
            # Ensure the app_settings singleton row exists before any schema
            # migration below runs. Migrations gate their one-time work on raw-SQL
            # *_done flags UPDATEd against this row; if the row doesn't exist yet,
            # the UPDATE affects 0 rows and silently no-ops, so a migration never
            # actually marks itself done and re-runs destructively on the next
            # restart. This must be a raw INSERT touching only the guaranteed-safe
            # `id` column — not AppSettings.get(), which SELECTs every ORM-mapped
            # column and raises "no such column" on upgrades where
            # ensure_runtime_schema() hasn't added the newest ones yet.
            from sqlalchemy import text as _text
            db.session.execute(_text("INSERT OR IGNORE INTO app_settings (id) VALUES (1)"))
            db.session.commit()
            ensure_runtime_schema()
        write_timezone_cache(AppSettings.get().timezone_name)

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
