import os

VERSION = "3.3.0"


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
    # isolation_level=None: use SQLite autocommit / deferred transactions so
    # gunicorn workers never issue BEGIN IMMEDIATE (which serialises all
    # connections behind a write lock).  WAL mode handles concurrency; readers
    # and writers proceed without blocking each other.
    SQLALCHEMY_ENGINE_OPTIONS = {
        'isolation_level': None,
        'connect_args': {'check_same_thread': False, 'timeout': 30},
    }
    REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
