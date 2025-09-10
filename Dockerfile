FROM mcr.microsoft.com/playwright/python:v1.55.0-noble

RUN apt-get update \
    && apt-get install -y tini \
    && rm -rf /var/lib/apt/lists/*

RUN pip install playwright \
    hatchet-sdk==1.18.1 \
    prisma \
    pymongo \
    dateparser \
    pillow \
    furl \
    && pip cache purge

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
CMD ["xvfb-run", "python", "worker.py"]

# CMD ["sh", "-c", "npx -y playwright@1.51.0 run-server --port 3000 --host 0.0.0.0"]
# CMD ["tail", "-f", "/dev/null"]
