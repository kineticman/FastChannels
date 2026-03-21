import os
from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix
from .extensions import db
from .config import Config, VERSION
from . import logfile
from .schema import ensure_runtime_schema
from .models import AppSettings
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
    # Trust X-Forwarded-Proto/Host from reverse proxies (Nginx, Traefik, Caddy, etc.)
    # so that request.host_url reflects the public https:// scheme instead of the
    # internal http:// connection, keeping logo proxy URLs scheme-correct in M3U output.
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
    app.config.from_object(config_class)
    _ensure_sqlite_parent_dir(app.config.get("SQLALCHEMY_DATABASE_URI"))

    @app.context_processor
    def inject_version():
        settings = AppSettings.get()
        return {
            'app_version': VERSION,
            'app_timezone_name': settings.effective_timezone_name(),
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

        # Fresh installs need the base tables before any startup path queries
        # AppSettings (for timezone cache, template globals, etc.).
        db.create_all()
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
