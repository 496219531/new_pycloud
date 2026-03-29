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
pycloud-control --role nodecontrol --bind 0.0.0.0:50061 --node-id node-local-01 \
  --infocenter-addr 127.0.0.1:50051 \
  --advertise-addr 127.0.0.1:50061
```

Windows install and run example:

```powershell
py -m pip install -e ".[grpc]"
py -m pycloud_parallel.controlplane.server --role infocenter --bind 0.0.0.0:50051
py -m pycloud_parallel.controlplane.server --role nodecontrol --bind 0.0.0.0:50061 --node-id node-win-01 --infocenter-addr 127.0.0.1:50051 --advertise-addr 127.0.0.1:50061
```

Contract docs:

- `GRPC_CONTRACT_V1.md`
- `proto/pycloud_v1.proto`
- `SERVICE_SESSION_PROTOCOL_V1.md`

Complex pure-client demo (requires InfoCenter + NodeControl running):

```bash
python scripts/grpc_client_complex_demo.py \
  --infocenter 127.0.0.1:50051 \
  --nodecontrol 127.0.0.1:50061 \
  --node-id node-local-01 \
  --client-id demo-client-01 \
  --tasks 120
```

Service-session demo (client uploads service code, keeps heartbeat, invokes over HTTP, then ends service):

```bash
python scripts/grpc_service_session_demo.py \
  --nodecontrol 127.0.0.1:50061 \
  --owner-client-id svc-owner-demo \
  --service-name square-service \
  --workers 4 \
  --heartbeat-timeout-sec 30 \
  --invoke-count 8
```

Multi-node service demo (discover nodes from InfoCenter, deploy to all healthy nodes, and invoke with load balancing):

```bash
python scripts/grpc_multi_node_service_demo.py \
  --infocenter 127.0.0.1:50051 \
  --owner-client-id svc-owner-multi-demo \
  --service-name square-service-multi \
  --workers 4 \
  --invoke-count 20 \
  --strategy least_inflight \
  --breaker-failure-threshold 3 \
  --breaker-cooldown-sec 15
```

Call existing deployed service directly (without re-registering service code):

```bash
python scripts/grpc_existing_service_client_demo.py \
  --infocenter 127.0.0.1:50051 \
  --service-name square-service-multi \
  --invoke-count 10
```

Note:
- Uploaded artifact must provide the configured entry function (default `run`).
- NodeControl executes that entry inside local subprocess workers.
- Service-session APIs (`CreateService`/`HeartbeatService`/`EndService`) are implemented in NodeControl.
- NodeControl can auto register/heartbeat to InfoCenter with deployed service routes.
- `MultiNodeServiceGroup.deploy_from_infocenter(...)` enforces unique `service_name` by default.
- Multi-node invoke has circuit-breaker recovery:
  - `breaker_failure_threshold`: consecutive failures before open-circuit
  - `breaker_cooldown_sec`: base open-circuit cooldown
  - `breaker_max_cooldown_sec`: max cooldown with exponential backoff

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
