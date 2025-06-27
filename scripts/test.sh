#!/usr/bin/env bash

PREFIX='uv run'

${PREFIX} pytest -s -x -vv --cov=src/ && ${PREFIX} coverage html