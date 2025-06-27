#!/usr/bin/env bash

PREFIX='uv run'

${PREFIX} ruff check . && ${PREFIX} ruff check . --diff