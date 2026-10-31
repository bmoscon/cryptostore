# syntax=docker/dockerfile:1
#
# Copyright (C) 2018-2026 Bryant Moscon - bmoscon@gmail.com
# Please see the LICENSE file for the terms and conditions associated with this software.
#
# Build arguments
#   PYTHON_VERSION   3.15 (default) or 3.15t for the free-threaded interpreter
#   UV_VERSION       the uv release used to install Python and the dependencies
#
#   docker build -t cryptostore .
#   docker build -t cryptostore:t --build-arg PYTHON_VERSION=3.15t .
#
# The interpreter is a uv-managed python-build-standalone build, so the same Dockerfile produces the
# free-threaded variant, which the official python images do not ship.

ARG PYTHON_VERSION=3.15
ARG UV_VERSION=0.13

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM debian:trixie-slim AS builder
ARG PYTHON_VERSION
COPY --from=uv /uv /uvx /bin/
ENV UV_PYTHON_INSTALL_DIR=/opt/python \
    UV_PYTHON_PREFERENCE=only-managed \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_NO_CACHE=1 \
    # aiokafka has no free-threaded wheel; build it without its C extension so it does not re-enable the GIL
    AIOKAFKA_NO_EXTENSIONS=1
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN uv python install ${PYTHON_VERSION}

WORKDIR /app
COPY pyproject.toml uv.lock README.md CHANGES.md LICENSE ./
COPY cryptostore ./cryptostore
# hiredis (redis-py's optional C parser) does not declare free-threading support and re-enables the GIL
# when imported, so the free-threaded image leaves it out - redis-py falls back to its pure python parser
RUN case "${PYTHON_VERSION}" in *t) NO_INSTALL="--no-install-package hiredis" ;; *) NO_INSTALL="" ;; esac \
    && uv sync --frozen --no-dev --no-editable --extra all --python ${PYTHON_VERSION} ${NO_INSTALL}


FROM debian:trixie-slim
ARG PYTHON_VERSION
LABEL org.opencontainers.image.title="cryptostore" \
      org.opencontainers.image.description="Config-driven cryptocurrency market data collector, storage and streaming service built on cryptofeed" \
      org.opencontainers.image.source="https://github.com/bmoscon/cryptostore" \
      org.opencontainers.image.licenses="XFree86-1.1"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system cryptostore \
    && useradd --system --gid cryptostore --home-dir /app --no-create-home cryptostore \
    && mkdir -p /app /config /data \
    && chown cryptostore:cryptostore /app /config /data

COPY --from=builder --chown=cryptostore:cryptostore /opt/python /opt/python
COPY --from=builder --chown=cryptostore:cryptostore /app/.venv /app/.venv

ENV PATH=/app/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    AIOKAFKA_NO_EXTENSIONS=1 \
    CRYPTOSTORE_CONFIG=/config/config.yaml \
    CRYPTOSTORE_PYTHON=${PYTHON_VERSION}

USER cryptostore
WORKDIR /data
VOLUME ["/config", "/data"]

# the stream backend's default example ports - publish the ones your config actually binds
EXPOSE 8080 5555

HEALTHCHECK --interval=60s --timeout=10s --start-period=120s --retries=3 CMD ["cryptostore", "health"]

ENTRYPOINT ["cryptostore"]
CMD ["run"]
