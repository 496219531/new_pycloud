#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  bash scripts/bump_and_build.sh [--dry-run] [--no-build]

Behavior:
  1. Read the current version from pyproject.toml and src/pycloud_parallel/__init__.py
  2. Increment the patch version by 1 (x.y.z -> x.y.(z+1))
  3. Update both files
  4. Run python -m build (unless --no-build is set)

Options:
  --dry-run   Print the current and next version without modifying files
  --no-build  Update the version but skip python -m build
  -h, --help  Show this help

Environment:
  PYTHON_BIN  Python executable to use (default: python)
EOF
}

DRY_RUN=0
NO_BUILD=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --no-build)
      NO_BUILD=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python}"

cd "${REPO_ROOT}"

CURRENT_VERSION="$("${PYTHON_BIN}" - <<'PY'
from pathlib import Path
import re

pyproject = Path("pyproject.toml").read_text(encoding="utf-8")
init_py = Path("src/pycloud_parallel/__init__.py").read_text(encoding="utf-8")

pyproject_match = re.search(r'^version\s*=\s*"([^"]+)"\s*$', pyproject, re.MULTILINE)
init_match = re.search(r'^__version__\s*=\s*"([^"]+)"\s*$', init_py, re.MULTILINE)

if pyproject_match is None:
    raise SystemExit("could not find project.version in pyproject.toml")
if init_match is None:
    raise SystemExit("could not find __version__ in src/pycloud_parallel/__init__.py")

pyproject_version = pyproject_match.group(1)
init_version = init_match.group(1)

if pyproject_version != init_version:
    raise SystemExit(
        f"version mismatch: pyproject.toml={pyproject_version} src/pycloud_parallel/__init__.py={init_version}"
    )

print(pyproject_version)
PY
)"

NEXT_VERSION="$("${PYTHON_BIN}" - "${CURRENT_VERSION}" <<'PY'
import re
import sys

current = sys.argv[1]
match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", current)
if match is None:
    raise SystemExit(f"unsupported version format: {current!r}")

major, minor, patch = (int(part) for part in match.groups())
print(f"{major}.{minor}.{patch + 1}")
PY
)"

echo "Current version: ${CURRENT_VERSION}"
echo "Next version:    ${NEXT_VERSION}"

if [[ "${DRY_RUN}" -eq 1 ]]; then
  exit 0
fi

"${PYTHON_BIN}" - "${CURRENT_VERSION}" "${NEXT_VERSION}" <<'PY'
from pathlib import Path
import re
import sys

current = sys.argv[1]
next_version = sys.argv[2]

targets = [
    Path("pyproject.toml"),
    Path("src/pycloud_parallel/__init__.py"),
]

patterns = {
    "pyproject.toml": re.compile(r'(^version\s*=\s*")([^"]+)("\s*$)', re.MULTILINE),
    "src/pycloud_parallel/__init__.py": re.compile(r'(^__version__\s*=\s*")([^"]+)("\s*$)', re.MULTILINE),
}

for path in targets:
    text = path.read_text(encoding="utf-8")
    pattern = patterns[str(path)]
    match = pattern.search(text)
    if match is None:
        raise SystemExit(f"could not update version in {path}")
    if match.group(2) != current:
        raise SystemExit(f"unexpected version in {path}: {match.group(2)!r} != {current!r}")
    updated = pattern.sub(lambda m: f"{m.group(1)}{next_version}{m.group(3)}", text, count=1)
    path.write_text(updated, encoding="utf-8")
PY

echo "Updated version files to ${NEXT_VERSION}"

if [[ "${NO_BUILD}" -eq 1 ]]; then
  exit 0
fi

"${PYTHON_BIN}" -m build

echo "Build complete."
ls -1 "dist" | grep "${NEXT_VERSION}" || true
