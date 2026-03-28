from __future__ import annotations

"""Cross-platform gRPC stub generator for PyCloud V1."""

import argparse
import subprocess
import sys
from pathlib import Path


def _rewrite_import(grpc_file: Path) -> None:
    text = grpc_file.read_text(encoding="utf-8")
    old = "import pycloud_v1_pb2 as pycloud__v1__pb2"
    new = "from . import pycloud_v1_pb2 as pycloud__v1__pb2"
    if old in text:
        text = text.replace(old, new)
        grpc_file.write_text(text, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate gRPC stubs for proto/pycloud_v1.proto")
    parser.add_argument("--root", default=None, help="Project root (defaults to script parent ..)")
    args = parser.parse_args()

    root = Path(args.root).resolve() if args.root else Path(__file__).resolve().parent.parent
    proto_dir = root / "proto"
    out_dir = root / "src" / "pycloud_parallel" / "grpc" / "v1"
    proto_file = proto_dir / "pycloud_v1.proto"

    out_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        "-I",
        str(proto_dir),
        f"--python_out={out_dir}",
        f"--grpc_python_out={out_dir}",
        str(proto_file),
    ]
    subprocess.run(cmd, check=True)

    grpc_file = out_dir / "pycloud_v1_pb2_grpc.py"
    _rewrite_import(grpc_file)
    print(f"Generated gRPC stubs under {out_dir}")


if __name__ == "__main__":
    main()
