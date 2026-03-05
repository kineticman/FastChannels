FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

EXPOSE 5523

# --timeout 300     : 5 min — covers worst-case first EPG generation with cold DB
# --keep-alive 5    : reuse connections from Channels DVR's repeat polls
# --workers 4       : more workers so one slow EPG gen doesn't block everything
# --worker-tmp-dir  : use tmpfs to avoid disk I/O on worker heartbeat files
CMD ["gunicorn", \
     "--bind", "0.0.0.0:5523", \
     "--workers", "4", \
     "--timeout", "300", \
     "--keep-alive", "5", \
     "--worker-tmp-dir", "/dev/shm", \
     "app:create_app()"]
