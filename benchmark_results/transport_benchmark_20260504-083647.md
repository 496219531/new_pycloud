# Transport Benchmark Report

- CSV: `/Users/hkk/Documents/new_pycloud/benchmark_results/transport_benchmark_20260504-083647.csv`
- Rows: 68
- Errors: 0

## Fastest total job p50
- `local_ipc` / `small_1kb`: p50 0.059 ms, p95 0.106 ms
- `local_ipc` / `light_x_plus_1`: p50 0.065 ms, p95 0.101 ms
- `local_ipc` / `medium_512kb`: p50 0.926 ms, p95 1.000 ms
- `http_nodecontrol` / `small_1kb`: p50 13.387 ms, p95 13.549 ms
- `http_nodecontrol` / `light_x_plus_1`: p50 13.788 ms, p95 14.928 ms
- `grpc_nodecontrol` / `small_1kb`: p50 14.052 ms, p95 14.765 ms
- `grpc_nodecontrol` / `light_x_plus_1`: p50 14.543 ms, p95 15.303 ms
- `grpc_nodecontrol` / `medium_512kb`: p50 15.844 ms, p95 16.854 ms

## Object upload p50
- `grpc_nodecontrol_object` / `medium_512kb`: p50 1.651 ms
- `http_object` / `medium_512kb`: p50 1.726 ms
- `grpc_nodecontrol_object` / `small_1kb`: p50 0.754 ms
- `http_object` / `small_1kb`: p50 0.774 ms

## Notes
- HTTP NodeControl currently pays JSON/base64/protobuf JSON conversion overhead, so small control calls may lag gRPC.
- HTTP object upload/download is the main apples-to-apples DataRef comparison against gRPC object streaming.
- `local_ipc` measures local service IPC calls, not remote NodeControl create/submit control-plane work.
- `bytes_copied_count` is an estimate from the benchmark harness, useful for comparing direction rather than exact allocator copies.
