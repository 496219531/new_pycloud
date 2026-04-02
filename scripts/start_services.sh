#!/bin/bash
# Thin wrapper around the installed/local pycloudctl CLI.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

export PYTHONPATH="$REPO_ROOT/src${PYTHONPATH:+:$PYTHONPATH}"
export PYCLOUD_HOME="${PYCLOUD_HOME:-$REPO_ROOT}"

exec python -m pycloud_parallel.controlplane.ctl "$@"
