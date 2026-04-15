#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import sys
from pathlib import Path


def _default_output_path(module_name: str) -> Path:
    return Path("/tmp/new_pycloud_package_debug") / f"{module_name}.tar.gz"


def main() -> int:
    parser = argparse.ArgumentParser(description="Build and inspect a local debug tar.gz package for a Python module.")
    parser.add_argument("module", help="Importable module name, for example: calc_asset_ratio_job_module")
    parser.add_argument(
        "--output",
        help="Output tar.gz path. Defaults to /tmp/new_pycloud_package_debug/<module>.tar.gz",
    )
    parser.add_argument(
        "--include-tests",
        action="store_true",
        help="Include test files when packaging.",
    )
    parser.add_argument(
        "--manifest",
        help="Optional manifest text file path. Defaults to <output>.contents.txt",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    src_dir = repo_root / "src"
    for path in (repo_root, src_dir):
        normalized = str(path)
        if normalized not in sys.path:
            sys.path.insert(0, normalized)

    from pycloud_parallel.controlplane.dependency import package_module_for_debug

    module = importlib.import_module(args.module)
    output_path = Path(args.output) if args.output else _default_output_path(args.module)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = Path(args.manifest) if args.manifest else output_path.with_suffix(output_path.suffix + ".contents.txt")
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    debug_info = package_module_for_debug(
        module,
        output_file=str(output_path),
        include_tests=bool(args.include_tests),
    )
    entries = [str(item) for item in debug_info["entries"]]
    manifest_path.write_text("\n".join(entries) + ("\n" if entries else ""), encoding="utf-8")

    print(f"package_path={debug_info['package_path']}")
    print(f"manifest_path={manifest_path.resolve()}")
    print(f"entry_count={len(entries)}")
    for item in entries:
        print(item)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
