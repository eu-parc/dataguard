#!/usr/bin/env bash

PREFIX='.venv/bin/python3.13 -m'

${PREFIX} ruff check . --fix && ${PREFIX} ruff format