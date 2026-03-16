import os
from flask import Flask
from .extensions import db
from .config import Config, VERSION
from . import logfile
from .schema import ensure_runtime_schema


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
    app.config.from_object(config_class)
    _ensure_sqlite_parent_dir(app.config.get("SQLALCHEMY_DATABASE_URI"))

    @app.context_processor
    def inject_version():
        return {'app_version': VERSION}

    db.init_app(app)
    with app.app_context():
        from sqlalchemy import event
        import sqlite3 as _sqlite3

        @event.listens_for(db.engine, "connect")
        def _set_sqlite_pragmas(dbapi_conn, _):
            if isinstance(dbapi_conn, _sqlite3.Connection):
                dbapi_conn.execute("PRAGMA journal_mode=WAL")
                dbapi_conn.execute("PRAGMA busy_timeout=5000")

        ensure_runtime_schema()

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
