# V1 Architecture Release Baseline

## Final Public Concepts

V1 freezes the user-facing model to five public concepts:

- `Service`
- `TaskPool`
- `JobQueue`
- `DataRef`
- `export`

Local-only parallel helpers remain available under `pycloud_parallel.local`, not the top-level package.

`Service.startup(...)` is the product-level path for startup-mounted services. It returns an internal startup-node handle that supports services attached when the process starts and rejects dynamic service deployment by default. `NodeControlState` extends that internal base and adds dynamic deployment support.

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
