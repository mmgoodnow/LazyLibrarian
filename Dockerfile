FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DOCKER=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libmagic1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir -r requirements.txt

COPY . .

RUN addgroup --system --gid 1000 lazylibrarian \
    && adduser --system --uid 1000 --ingroup lazylibrarian lazylibrarian \
    && mkdir -p /config \
    && chown -R lazylibrarian:lazylibrarian /config

USER 1000:1000

EXPOSE 5299
VOLUME ["/config"]

CMD ["python", "-u", "LazyLibrarian.py", "--datadir", "/config", "--config", "/config/config.ini", "--nolaunch"]

