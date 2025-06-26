#!/usr/bin/env bash

set -e

SCRIPT_DIR="scripts"

echo "Running pre-commit checks..."

echo "1. Formatting code..."
"${SCRIPT_DIR}/format.sh"
if [ $? -ne 0 ]; then
    echo "❌ Formatting failed"
    exit 1
fi
echo "✅ Formatting completed"

echo "2. Linting code..."
"${SCRIPT_DIR}/lint.sh"
if [ $? -ne 0 ]; then
    echo "❌ Linting failed"
    exit 1
fi
echo "✅ Linting completed"

echo "3. Running tests..."
"${SCRIPT_DIR}/test.sh"
if [ $? -ne 0 ]; then
    echo "❌ Tests failed"
    exit 1
fi
echo "✅ Tests completed"

echo "🎉 All pre-commit checks passed!"