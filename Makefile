IMAGE ?= ghcr.io/bmoscon/cryptostore
VERSION := $(shell uv version --short)

.PHONY: sync lint test test-ft build build-ft run check

sync:
	uv sync --all-extras

lint:
	uv run ruff check .

test:
	uv run pytest

# the free-threaded interpreter, in its own environment. hiredis re-enables the GIL so it is left out,
# and `uv run --no-sync` keeps uv from putting it back
test-ft:
	UV_PROJECT_ENVIRONMENT=.venv-t AIOKAFKA_NO_EXTENSIONS=1 uv sync --python 3.15t --all-extras --no-install-package hiredis
	UV_PROJECT_ENVIRONMENT=.venv-t AIOKAFKA_NO_EXTENSIONS=1 uv run --no-sync pytest

build:
	docker build . -t $(IMAGE):$(VERSION) -t $(IMAGE):latest

build-ft:
	docker build . --build-arg PYTHON_VERSION=3.15t -t $(IMAGE):$(VERSION)-t -t $(IMAGE):latest-t

check:
	uv run cryptostore check $(CONFIG)

run:
	uv run cryptostore run $(CONFIG)
