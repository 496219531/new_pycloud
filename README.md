# pycloud-parallel

`pycloud-parallel` is a Python 3.8+ package for low-intrusion loop parallelization.

> 中文说明：这个包的目标是“尽量少改原代码”把 `for` 循环并行化，并支持多集群与多项目并行。

## Highlights

- Decorator-first API: `@parallel_for(...)`
- Explicit fallback API: `foreach(iterable, fn, ...)`
- Multi-cluster routing with weighted least-load policy
- Multi-project concurrency isolation with per-project CPU quota
- Error semantics: skip (default) or raise
- Result semantics: ordered (default) or as-completed

## Quick start

```python
from pc import parallel_for

@parallel_for(mode="ordered", on_error="skip", retries=1, project="default")
def calc(nums):
    out = []
    for n in nums:
        out.append(n * n)
    return out

print(calc(list(range(10))))
```

## Benchmark

```bash
python benchmarks/cpu_benchmark.py --size 200 --clusters 2 --capacity 4
```

## Config file (`pycloud.yaml`)

```yaml
clusters:
  - name: local
    address: local
    weight: 1.0
    capacity: 8
projects:
  default:
    cpu_quota: 8
    mem_quota: 0
    priority: 1
    default_retries: 0
    default_on_error: skip
default_project: default
```

## gRPC Control Plane (V1)

Generate protobuf stubs:

```bash
bash scripts/gen_grpc_stubs.sh
```

Windows (PowerShell):

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\gen_grpc_stubs.ps1
```

Cross-platform (Python direct):

```bash
python scripts/gen_grpc_stubs.py
```

Start InfoCenter:

```bash
pycloud-control --role infocenter --bind 0.0.0.0:50051
```

Start NodeControl (same binary, different role):

```bash
pycloud-control --role nodecontrol --bind 0.0.0.0:50061 --node-id node-local-01
```

Windows install and run example:

```powershell
py -m pip install -e ".[grpc]"
py -m pycloud_parallel.controlplane.server --role infocenter --bind 0.0.0.0:50051
py -m pycloud_parallel.controlplane.server --role nodecontrol --bind 0.0.0.0:50061 --node-id node-win-01
```

Contract docs:

- `GRPC_CONTRACT_V1.md`
- `proto/pycloud_v1.proto`

## Local Chat Backup (Codex)

Sync all Codex session logs from `~/.codex/sessions` into this repo:

```bash
python3 scripts/sync_codex_chat_logs.py --workspace .
```

Enable auto-sync every 60 seconds on macOS (`launchd`):

```bash
bash scripts/install_chatlog_sync_launchd.sh ~/.codex/chat_backup .
```

This creates a project link at `chat_logs/auto_sessions` that points to the auto-updated archive.

Disable auto-sync:

```bash
bash scripts/uninstall_chatlog_sync_launchd.sh ~/.codex/chat_backup .
```
