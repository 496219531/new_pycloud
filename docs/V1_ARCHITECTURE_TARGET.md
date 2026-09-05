# V1 Architecture Release Baseline

## Final Public Concepts

V1 freezes the user-facing model to five public concepts:

- `Service`
- `TaskPool`
- `JobQueue`
- `DataRef`
- `export`

Old local-only parallel helpers are removed from V1; execution goes through `Service`, `TaskPool`, or `JobQueue`.

`Service.startup(...)` is the product-level path for startup-mounted services. It returns an internal startup-node handle that supports services attached when the process starts and rejects dynamic service deployment by default. `startup.foo.sync(...)` is an in-process convenience proxy to that node's mounted service: it calls the local executor queue through `StartupServiceNode.call_service(...)` and does not go through Discovery, Gateway, or service HTTP. Startup control methods such as `update_globals(...)` remain local node methods. `NodeControlState` extends that internal base and adds dynamic deployment support.

`target="local"` is the explicit local IPC runtime. `Service.startup(target="local", ...)` and `Service.deploy(target="local", ...)` both return a local owner proxy and publish a same-machine IPC registry entry for `Service.connect(target="local", service_name=...)`, but their creation semantics are intentionally different. `startup(local)` and default `deploy(local)` are module-first: they create an internal `ExecuteSpec(source_kind="module_import")`, skip business-source packaging and upload, and let local `ProcessPoolExecutor` workers import from the recorded local import root. Explicit artifact/deps/resource/package arguments use `source_kind="artifact"`. Connected local IPC calls avoid network transport but retain the executor-host/process-worker boundary so CPU-bound work honors `worker_count`. Local mode keeps the same user-side method proxy shape, including streaming calls, but always uses `pickle_native_v1` for local IPC compatibility and ignores external `serialization_mode` preferences. Broadcast on a local or connected service is treated as a single-node call and returns one result entry. `JobQueue.connect("local", ...)` reuses the same service connect local IPC route to call a local `job-orchestrator`; for `JobQueue.submit(target="local", source=module)`, the client sends import metadata instead of packaging code, and the job-orchestrator process imports that module and opens a local `TaskPool` directly. `TaskPool.open(target="local", ...)` creates a private opener-owned `NodeControlState` and uses the same `ExecutorHost + ProcessPoolExecutor` runtime for module, callable, and artifact sources; the former direct `ThreadPoolExecutor` implementation has been removed. Empty target remains startup-only unregistered mode and does not imply local mode.

`Service.connect(...)` is always a caller-side client, for both local and remote routes. It does not expose owner/control operations such as `update_globals(...)`; those remain available only on handles returned by `Service.startup(...)` or `Service.deploy(...)`.

Local TaskPool is a private single-machine pool. If its local worker or pool state is lost, it fails fast and leaves rebuild/retry to the opener instead of running the distributed accepted-task replay path. Windows named pipe, spawn behavior, and Ctrl+C cleanup are platform validation items and should be tested on Windows directly.

`job-orchestrator` is implemented as a built-in startup service module, not as a separate communication stack. The server process hosts a `StartupServiceNode`, mounts `pycloud_parallel.controlplane.job_orchestrator_service`, and then exposes the normal startup service HTTP/local IPC endpoints. The module owns queue business logic; the protocol remains HTTP for remote calls.

## Concepts Removed From The Final Public Surface

These legacy categories are not part of the V1 public surface:

- legacy owner-side deploy facade
- legacy gateway/direct caller facades
- legacy queue client naming
- legacy task-pool session naming
- legacy dedicated service-backed task facade

## Execution Target

- `ExecutorHost` is the shared execution foundation.
- `ExecutionSession` is the shared internal session model.
- `Service` and `TaskPool` are sibling product-level session types built on that shared foundation.
- `JobQueue` only schedules and launches `TaskPool` work; it does not own a separate execution worker model.

## Data Target

- Large inputs and large results both converge on `DataRef`.
- `DataRef` is the only public large-object reference type.
- Object upload, gateway upload-call staging, job delayed resolve, and result materialization all converge on the same `DataRef` data plane.
- Internal trusted `DataRef` flow is upload-once by default: the client uploads one object copy, intermediate control layers forward the reference, and the final worker or client fetches/materializes from the locator or registry.
- Gateway/public `DataRef` trust and relay behavior remains a separate boundary; gateway relay keeps its eager default until that boundary is explicitly redesigned.

## Runtime Contract

- Python runtime incompatibility must fail before execution starts.
- The error must always include:
  - requested runtime
  - discovered node Python versions
  - a concrete repair suggestion

## Package Layout Target

V1 stable ownership is organized around these package roles:

- `pycloud_parallel.api`
- `pycloud_parallel.data`
- `pycloud_parallel.artifact`
- `pycloud_parallel.runtime`
- `pycloud_parallel.execution`

The `controlplane/` package remains available for internal infrastructure and advanced integrations, but product-facing examples and docs should prefer the public concepts above.

## Release Acceptance

Before cutting a V1 release, keep these checks green:

1. Top-level imports expose only `Service`, `TaskPool`, `JobQueue`, `DataRef`, and `export`.
2. Public docs and examples use `Service.connect(...)`, `Service.deploy(...)`, `Service.startup(...)`, `TaskPool`, and `JobQueue.submit(source=...)` as the main paths.
3. Compatibility helpers remain documented only as advanced or internal paths when they still exist.
4. Large payload and large result examples converge on `DataRef`.
5. Release docs match the default policy/mode bindings in `policy_profile.py` and runtime defaults in `config.py`.
