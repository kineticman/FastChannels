import os

VERSION = "1.3.1"


class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret')
    PUBLIC_BASE_URL = os.environ.get('PUBLIC_BASE_URL', '')
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
        'connect_args': {'check_same_thread': False},
    }
    REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
