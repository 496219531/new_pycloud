# Transport Benchmark Report

- CSV: `/Users/hkk/Documents/new_pycloud/benchmark_results_full/transport_benchmark_20260504-084402.csv`
- Rows: 132
- Errors: 777

## Fastest total job p50
- `local_ipc` / `light_x_plus_1`: p50 0.052 ms, p95 0.056 ms
- `local_ipc` / `small_1kb`: p50 0.067 ms, p95 0.219 ms
- `local_ipc` / `medium_512kb`: p50 1.327 ms, p95 1.916 ms
- `local_ipc` / `dataframe_100k`: p50 3.709 ms, p95 4.329 ms
- `grpc_nodecontrol` / `small_1kb`: p50 12.344 ms, p95 12.856 ms
- `grpc_nodecontrol` / `light_x_plus_1`: p50 13.096 ms, p95 13.952 ms
- `grpc_nodecontrol` / `medium_512kb`: p50 13.856 ms, p95 14.536 ms
- `grpc_taskpool` / `medium_512kb`: p50 52.645 ms, p95 54.158 ms

## Object upload p50
- `grpc_nodecontrol_object` / `large_8mb`: p50 19.336 ms
- `http_object` / `large_8mb`: p50 18.650 ms
- `grpc_nodecontrol_object` / `medium_512kb`: p50 1.810 ms
- `http_object` / `medium_512kb`: p50 1.873 ms
- `grpc_nodecontrol_object` / `small_1kb`: p50 0.808 ms
- `http_object` / `small_1kb`: p50 0.839 ms

## Notes
- HTTP NodeControl currently pays JSON/base64/protobuf JSON conversion overhead, so small control calls may lag gRPC.
- HTTP object upload/download is the main apples-to-apples DataRef comparison against gRPC object streaming.
- `local_ipc` measures local service IPC calls, not remote NodeControl create/submit control-plane work.
- `bytes_copied_count` is an estimate from the benchmark harness, useful for comparing direction rather than exact allocator copies.
