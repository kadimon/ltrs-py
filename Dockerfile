FROM mcr.microsoft.com/playwright/python:v1.55.0-noble

RUN apt-get update \
    && apt-get install -y tini \
    && rm -rf /var/lib/apt/lists/*

RUN pip install playwright \
    patchright \
    hatchet-sdk==1.19.0 \
    prisma \
    pymongo \
    dateparser \
    pillow \
    puremagic \
    aiobotocore \
    furl \
    pandas \
    ultimate-sitemap-parser \
    && pip cache purge

WORKDIR /app

COPY ./schema.prisma ./
RUN prisma generate --generator client-py

COPY ./workflows/ ./workflows/
COPY ./policies.json \
    ./worker.py \
    ./settings.py \
    ./db.py \
    ./interfaces.py \
    ./workflow_base.py \
    ./utils.py \
    ./

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["xvfb-run", "-a", "python", "worker.py"]

# CMD ["sh", "-c", "npx -y playwright@1.51.0 run-server --port 3000 --host 0.0.0.0"]
# CMD ["tail", "-f", "/dev/null"]
