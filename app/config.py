import os

VERSION = "5.8.0"


class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret')
    PUBLIC_BASE_URL = os.environ.get('PUBLIC_BASE_URL', '')
    VERSION_CHECK_ENABLED = (os.environ.get('VERSION_CHECK_ENABLED', '1').strip() != '0')
    VERSION_CHECK_REPO = os.environ.get('VERSION_CHECK_REPO', 'kineticman/FastChannels').strip() or 'kineticman/FastChannels'
    VERSION_CHECK_TTL_HOURS = int((os.environ.get('VERSION_CHECK_TTL_HOURS', '12') or '12').strip() or '12')
    SQLALCHEMY_DATABASE_URI = os.environ.get(
        'DATABASE_URL',
        'sqlite:////data/fastchannels.db'
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    TEMPLATES_AUTO_RELOAD = os.environ.get('FLASK_DEBUG', '0') == '1'
    # isolation_level=None leaves pysqlite in its legacy mode (not autocommit):
    # it opens a deferred transaction at the first INSERT/UPDATE/DELETE and holds
    # the database-wide writer lock until commit, so keep write transactions
    # short (app/db_lock_watch.py logs long ones). WAL lets readers proceed
    # alongside the one writer.
    SQLALCHEMY_ENGINE_OPTIONS = {
        'isolation_level': None,
        'connect_args': {'check_same_thread': False, 'timeout': 30},
    }
    REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
