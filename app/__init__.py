from flask import Flask
from .extensions import db
from .config import Config, VERSION
from . import logfile
from .schema import ensure_runtime_schema


def create_app(config_class=Config):
    logfile.setup()
    app = Flask(__name__)
    app.config.from_object(config_class)

    @app.context_processor
    def inject_version():
        return {'app_version': VERSION}

    db.init_app(app)
    with app.app_context():
        ensure_runtime_schema()

    from .routes.output import output_bp
    from .routes.api import api_bp
    from .routes.feeds_api import feeds_api_bp
    from .routes.admin import admin_bp
    from .routes.play import play_bp

    app.register_blueprint(output_bp)
    app.register_blueprint(api_bp, url_prefix='/api')
    app.register_blueprint(feeds_api_bp, url_prefix='/api/feeds')
    app.register_blueprint(admin_bp, url_prefix='/admin')
    app.register_blueprint(play_bp)   # /play/<source>/<id>.m3u8

    return app
