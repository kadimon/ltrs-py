FROM python:3.13.14-bookworm

RUN apt-get update \
    && apt-get install -y tini \
    && rm -rf /var/lib/apt/lists/*

RUN pip install playwright \
    cloverlabs-camoufox[geoip] \
    playwright==1.59.0 \
    hatchet-sdk==1.33.10 \
    prisma \
    pymongo \
    dateparser \
    pillow \
    puremagic \
    aiobotocore \
    furl \
    pandas \
    ultimate-sitemap-parser \
    # croniter \
    && pip cache purge

RUN playwright install-deps firefox
RUN python -m camoufox fetch

WORKDIR /app

COPY ./schema.prisma ./
RUN prisma generate --generator client-py

COPY ./workflows/ ./workflows/
COPY ./worker.py \
    ./settings.py \
    ./db.py \
    ./interfaces.py \
    ./workflow_base.py \
    ./utils.py \
    ./

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python3", "worker.py"]
