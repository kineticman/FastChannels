from gevent import monkey

# Gunicorn gevent workers warn if ssl is imported before monkey-patching.
# Keep preload enabled, but patch at the earliest possible import point.
monkey.patch_all()

from app import create_app

app = create_app()
