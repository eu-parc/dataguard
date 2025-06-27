#!/usr/bin/env bash

PREFIX='uv run'

${PREFIX} ruff check . --fix && ${PREFIX} ruff format