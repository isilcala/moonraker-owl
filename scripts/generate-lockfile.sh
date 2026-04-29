#!/usr/bin/env bash
# =============================================================================
# Generate / refresh requirements.lock.txt for moonraker-owl (audit A-07).
#
# We use `uv` for hash-pinned reproducible installs because pip-tools'
# `pip-compile` does not natively support hash generation across the same
# matrix uv resolves.
#
# Prerequisite: install uv (https://docs.astral.sh/uv/)
#   curl -LsSf https://astral.sh/uv/install.sh | sh
#
# Usage (from repository root):
#   ./Owl.Moonraker/scripts/generate-lockfile.sh
#
# Commit the resulting requirements.lock.txt. Operator-facing install.sh
# uses it via `pip install --require-hashes -r requirements.lock.txt`.
# =============================================================================
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v uv >/dev/null 2>&1; then
  echo "error: uv is not installed. See https://docs.astral.sh/uv/." >&2
  exit 1
fi

uv pip compile pyproject.toml \
  --output-file requirements.lock.txt \
  --generate-hashes \
  --no-emit-index-url \
  --quiet

echo "Generated requirements.lock.txt with $(grep -c '^[a-zA-Z0-9]' requirements.lock.txt) pinned packages."
