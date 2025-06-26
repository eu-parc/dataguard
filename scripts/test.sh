#!/usr/bin/env bash

PREFIX='.venv/bin/python3.13 -m'

${PREFIX} pytest -s -x -vv --cov=src/ && ${PREFIX} coverage html