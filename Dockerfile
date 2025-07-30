FROM mcr.microsoft.com/playwright/python:v1.54.0-noble

RUN apt-get update \
    && apt-get install -y tini \
    && rm -rf /var/lib/apt/lists/*

RUN pip install playwright \
    hatchet-sdk==1.15.1 \
    prisma \
    pymongo \
    && pip cache purge

WORKDIR /app

# COPY ./prisma ./prisma
# RUN prisma generate --generator client-py

COPY ./workflows/ ./workflows/
COPY ./worker.py \
    ./settings.py \
    ./

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["xvfb-run", "python", "worker.py"]

# CMD ["sh", "-c", "npx -y playwright@1.51.0 run-server --port 3000 --host 0.0.0.0"]
# CMD ["tail", "-f", "/dev/null"]
