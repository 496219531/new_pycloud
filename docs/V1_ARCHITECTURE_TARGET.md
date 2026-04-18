# V1 Architecture Target

## Final Public Concepts

V1 freezes the user-facing model to five concepts only:

- `Service`
- `TaskPool`
- `JobQueue`
- `DataRef`
- `export`

Local-only parallel helpers remain available under `pycloud_parallel.local`, not the top-level package.

## Concepts Removed From The Final Public Surface

These legacy categories are transitional and must not survive the V1 cleanup:

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

## Runtime Contract

- Python runtime incompatibility must fail before execution starts.
- The error must always include:
  - requested runtime
  - discovered node Python versions
  - a concrete repair suggestion

## Package Layout Target

V1 code is being split toward these stable package roles:

- `pycloud_parallel.api`
- `pycloud_parallel.data`
- `pycloud_parallel.artifact`
- `pycloud_parallel.runtime`
- `pycloud_parallel.execution`

The current `controlplane/` package may continue to exist during migration, but the target ownership for new stable code is the directories above.

## Migration Sequence

1. Freeze docs, validation rules, and V1 acceptance tests.
2. Unify large payloads and large results under `DataRef`.
3. Unify execution foundation under `ExecutorHost + ExecutionSession`.
4. Switch the top-level package to the final public API.
5. Remove legacy names and rewrite docs/examples to match the final surface.
