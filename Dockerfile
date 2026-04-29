FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    redis-server \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt
# Keep yt-dlp at GitHub master — YouTube extraction breaks on stale PyPI releases
RUN pip install --force-reinstall "yt-dlp[default] @ https://github.com/yt-dlp/yt-dlp/archive/master.tar.gz"
RUN playwright install-deps chromium && playwright install chromium

COPY . .

RUN chmod +x /app/entrypoint.sh

EXPOSE 5523

ENTRYPOINT ["/app/entrypoint.sh"]
