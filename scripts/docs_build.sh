#!/usr/bin/env bash

PREFIX='uv run --no-default-groups --group docs'

${PREFIX} zensical build --clean --strict
