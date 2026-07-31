# Official RunPod base published from https://github.com/runpod/containers.
# Resolved for linux/amd64 on 2026-07-31 with:
# docker buildx imagetools inspect runpod/base:1.1.0-ubuntu2404
FROM runpod/base:1.1.0-ubuntu2404@sha256:8fafffd0c67a48117dff09031ed16e79c0a83cc6996472f4b4dcb76343102a66

ARG SOURCE_DATE_EPOCH=0

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
        python3-venv \
        tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp/runpod-jobrunner-build
COPY pyproject.toml README.md ./
COPY src ./src
RUN python3 -m venv /opt/runpod-jobrunner/venv \
    && SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH}" \
        /opt/runpod-jobrunner/venv/bin/python -m pip install --no-cache-dir . \
    && rm -rf /tmp/runpod-jobrunner-build

COPY --chmod=0555 container/entrypoint.sh /opt/runpod-jobrunner/entrypoint
COPY --chmod=0555 container/noop.py /opt/runpod-jobrunner/noop

ENV PATH="/opt/runpod-jobrunner/venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

LABEL org.opencontainers.image.title="runpod-jobrunner no-op runner" \
      org.opencontainers.image.source="https://github.com/halbritt/runpod-jobrunner"

EXPOSE 22 8080

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/runpod-jobrunner/entrypoint"]
