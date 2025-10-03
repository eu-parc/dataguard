#!/usr/bin/env bash

PREFIX='uv run'

${PREFIX} pytest -s -vv --cov=src/ && ${PREFIX} coverage html