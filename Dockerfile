FROM python:3.13.14-bookworm

RUN apt-get update \
    && apt-get install -y tini \
    && rm -rf /var/lib/apt/lists/*

RUN pip install \
    playwright==1.60.0 \
    cloverlabs-camoufox[geoip] \
    hatchet-sdk==1.40.1 \
    prisma \
    pymongo \
    httpx[socks] \
    dateparser \
    pillow \
    puremagic \
    aiobotocore \
    furl \
    pandas \
    ultimate-sitemap-parser \
    # croniter \
    && pip cache purge

RUN python -m playwright install-deps firefox
RUN python -m camoufox sync \
    && python -m camoufox set official/prerelease \
    && python -m camoufox fetch

WORKDIR /app

COPY ./schema.prisma ./
RUN python -m prisma generate --generator client-py

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
