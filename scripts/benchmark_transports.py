#!/usr/bin/env python3
from __future__ import annotations

"""Manual transport benchmark for release checks.

This script is intentionally outside normal CI. It starts one in-process
NodeControl state, exposes it through gRPC, HTTP NodeControl, and HTTP object
servers, then runs the same payload cases through each available lane.
"""

import argparse
import csv
import gc
import json
import math
import os
import pickle
import statistics
import sys
import tempfile
import time
from concurrent import futures
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
for candidate in (str(ROOT), str(SRC)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

import grpc

from pycloud_parallel.controlplane.config import grpc_channel_options
from pycloud_parallel.controlplane.local_ipc import LocalServiceClient, start_local_service_ipc
from pycloud_parallel.controlplane.node_control_client import NodeControlClient
from pycloud_parallel.controlplane.node_control_http import HttpNodeControlClient, NodeControlHttpServer
from pycloud_parallel.controlplane.node_object_http import HttpNodeObjectClient, NodeObjectHttpServer
from pycloud_parallel.controlplane.nodecontrol_state import NodeControlState
from pycloud_parallel.controlplane.payload_transport import decode_result_from_transport
from pycloud_parallel.controlplane.serialization import (
    decode_inline_transport_carrier,
    decode_transport_payload_bytes,
    detect_transport_mode,
    struct_to_python,
)
from pycloud_parallel.controlplane.services import NodeControlService
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2_grpc as pb2_grpc


SERVICE_SOURCE = b"""
import time


def _payload_size(value):
    if value is None:
        return 0
    if isinstance(value, (bytes, bytearray, memoryview)):
        return len(value)
    shape = getattr(value, "shape", None)
    if shape is not None:
        try:
            return int(getattr(value, "memory_usage", lambda **_: [0])(deep=True).sum())
        except Exception:
            pass
        try:
            return int(value.nbytes)
        except Exception:
            pass
    try:
        return len(value)
    except Exception:
        return 1


def run(x=0, blob=None, value=None, sleep_sec=0.0, cpu_sec=0.0, **_kwargs):
    sleep = float(sleep_sec or 0.0)
    if sleep > 0:
        time.sleep(sleep)
    cpu = float(cpu_sec or 0.0)
    if cpu > 0:
        end = time.perf_counter() + cpu
        acc = 0
        while time.perf_counter() < end:
            acc = (acc * 1315423911 + 1) & 0xFFFFFFFF
    item = blob if blob is not None else value
    return {"value": int(x or 0) + 1, "payload_size": _payload_size(item)}
"""


@dataclass(frozen=True)
class Case:
    name: str
    payload_factory: Callable[[], Dict[str, Any]]
    payload_size: int
    timeout_sec: float = 30.0


@dataclass
class Sample:
    transport: str
    case: str
    metric: str
    payload_size: int
    result_size: int
    values_ms: List[float]
    error_count: int = 0
    bytes_copied_count: int = 0


class BenchNode:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.state = NodeControlState(
            node_id="bench-node",
            worker_capacity=8,
            queue_capacity=2048,
            artifact_dir=str(root / "node"),
            service_http_bind="127.0.0.1:0",
            service_http_base_url="",
            task_pool_worker_capacity=4,
            service_worker_capacity=4,
        )
        self.grpc_server: Optional[grpc.Server] = None
        self.grpc_target = ""
        self.http_control: Optional[NodeControlHttpServer] = None
        self.http_object: Optional[NodeObjectHttpServer] = None

    def start(self) -> None:
        self.grpc_server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=32),
            options=grpc_channel_options(),
        )
        pb2_grpc.add_NodeControlServiceServicer_to_server(NodeControlService(self.state), self.grpc_server)
        port = self.grpc_server.add_insecure_port("127.0.0.1:0")
        self.grpc_server.start()
        self.grpc_target = f"127.0.0.1:{port}"

        self.http_control = NodeControlHttpServer(bind="127.0.0.1:0", state=self.state)
        self.http_control.start()
        self.state.node_http_base_url = self.http_control.base_url

        self.http_object = NodeObjectHttpServer(bind="127.0.0.1:0", state=self.state)
        self.http_object.start()

    @property
    def http_control_url(self) -> str:
        assert self.http_control is not None
        return self.http_control.base_url

    @property
    def http_object_url(self) -> str:
        assert self.http_object is not None
        return self.http_object.base_url

    def close(self) -> None:
        if self.http_object is not None:
            self.http_object.stop()
        if self.http_control is not None:
            self.http_control.stop()
        if self.grpc_server is not None:
            self.grpc_server.stop(grace=0)
        self.state.close()


class LocalIpcNode:
    node_id = "bench-local-node"
    node_instance_id = "bench-local-node-inst"
    methods = ["run"]

    def __init__(self, object_dir: Path) -> None:
        self.object_dir = object_dir

    def call_balanced(self, method: str, payload: Dict[str, Any], **kwargs: Any):
        del method, kwargs
        decoded = decode_inline_transport_carrier(payload, context="service_owner")
        sleep_sec = float(decoded.get("sleep_sec") or 0.0)
        if sleep_sec > 0:
            time.sleep(sleep_sec)
        item = decoded.get("blob", decoded.get("value"))
        size = len(item) if isinstance(item, (bytes, bytearray, memoryview)) else 0
        return self.node_instance_id, {"ok": True, "data": {"value": int(decoded.get("x") or 0) + 1, "payload_size": size}}


def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return math.nan
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * pct
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return ordered[lo]
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (pos - lo)


def _payload_wire_size(payload: Dict[str, Any]) -> int:
    try:
        return len(pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL))
    except Exception:
        return 0


def _result_wire_size(value: Any) -> int:
    try:
        return len(pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL))
    except Exception:
        return 0


def _decode_service_response(response: pb2.CallServiceResponse) -> Any:
    if response.HasField("transport_data") and str(response.transport_data.codec or "").strip():
        return decode_transport_payload_bytes(
            str(response.transport_data.codec or ""),
            int(response.transport_data.version or 0),
            response.transport_data.payload,
            context="service_owner",
        )
    raw = struct_to_python(response.data)
    return decode_result_from_transport(
        raw,
        mode=detect_transport_mode(raw, default="legacy_v1"),
        context="service_owner",
    )


def _decode_task_result(result: pb2.TaskResult) -> Any:
    if result.HasField("transport_result") and str(result.transport_result.codec or "").strip():
        return decode_transport_payload_bytes(
            str(result.transport_result.codec or ""),
            int(result.transport_result.version or 0),
            result.transport_result.payload,
            context="taskpool_session",
        )
    raw = struct_to_python(result.result)
    return decode_result_from_transport(
        raw,
        mode=detect_transport_mode(raw, default="legacy_v1"),
        context="taskpool_session",
    )


def _record(samples: List[Sample], *, transport: str, case: str, metric: str, payload_size: int, result_size: int, value_ms: float, bytes_copied_count: int = 0) -> None:
    for sample in samples:
        if sample.transport == transport and sample.case == case and sample.metric == metric:
            sample.values_ms.append(value_ms)
            sample.result_size = max(sample.result_size, result_size)
            sample.bytes_copied_count = max(sample.bytes_copied_count, bytes_copied_count)
            return
    samples.append(
        Sample(
            transport=transport,
            case=case,
            metric=metric,
            payload_size=payload_size,
            result_size=result_size,
            values_ms=[value_ms],
            bytes_copied_count=bytes_copied_count,
        )
    )


def _record_error(samples: List[Sample], *, transport: str, case: str, metric: str, payload_size: int) -> None:
    for sample in samples:
        if sample.transport == transport and sample.case == case and sample.metric == metric:
            sample.error_count += 1
            return
    samples.append(Sample(transport=transport, case=case, metric=metric, payload_size=payload_size, result_size=0, values_ms=[], error_count=1))


def make_cases(full: bool, include_optional: bool) -> List[Case]:
    cases = [
        Case("small_1kb", lambda: {"blob": b"a" * 1024}, 1024),
        Case("medium_512kb", lambda: {"blob": b"b" * (512 * 1024)}, 512 * 1024),
        Case("light_x_plus_1", lambda: {"x": 1}, _payload_wire_size({"x": 1})),
        Case("sleep_1s", lambda: {"x": 1, "sleep_sec": 1.0}, _payload_wire_size({"x": 1, "sleep_sec": 1.0}), timeout_sec=10.0),
    ]
    if full:
        cases.extend(
            [
                Case("large_8mb", lambda: {"blob": b"c" * (8 * 1024 * 1024)}, 8 * 1024 * 1024),
                Case("sleep_5s", lambda: {"x": 1, "sleep_sec": 5.0}, _payload_wire_size({"x": 1, "sleep_sec": 5.0}), timeout_sec=15.0),
            ]
        )
    if include_optional:
        try:
            import pandas as pd

            def dataframe_payload() -> Dict[str, Any]:
                return {"value": pd.DataFrame({"a": range(100_000), "b": range(100_000)})}

            cases.append(Case("dataframe_100k", dataframe_payload, _payload_wire_size(dataframe_payload())))
        except Exception:
            pass
        try:
            import numpy as np

            def ndarray_payload() -> Dict[str, Any]:
                return {"value": np.arange(4_000_000, dtype="float64")}

            cases.append(Case("ndarray_32mb", ndarray_payload, _payload_wire_size(ndarray_payload())))
        except Exception:
            pass
    return cases


def object_payload_cases(cases: Iterable[Case]) -> List[Case]:
    selected = []
    for case in cases:
        payload = case.payload_factory()
        blob = payload.get("blob")
        if isinstance(blob, (bytes, bytearray, memoryview)):
            selected.append(case)
    return selected


def run_object_bench(samples: List[Sample], node: BenchNode, cases: List[Case], *, warmup: int, repeat: int) -> None:
    clients = {
        "grpc_nodecontrol_object": NodeControlClient(node.grpc_target, timeout_sec=60.0),
        "http_object": HttpNodeObjectClient(node.http_object_url, timeout_sec=60.0),
    }
    try:
        for transport, client in clients.items():
            for case in object_payload_cases(cases):
                iterations = warmup + repeat
                for index in range(iterations):
                    payload = case.payload_factory()
                    blob = bytes(payload["blob"])
                    measured = index >= warmup
                    try:
                        start = time.perf_counter()
                        ref = client.upload_object_from_bytes(blob=blob, format="bin", trusted_precheck=False, transfer_mode="single_pass_authoritative")
                        upload_ms = (time.perf_counter() - start) * 1000.0
                        start = time.perf_counter()
                        downloaded = client.download_object_bytes(object_id=ref.object_id)
                        download_ms = (time.perf_counter() - start) * 1000.0
                        if downloaded != blob:
                            raise RuntimeError("downloaded object mismatch")
                        if measured:
                            _record(samples, transport=transport, case=case.name, metric="upload_ms", payload_size=len(blob), result_size=ref.size_bytes, value_ms=upload_ms, bytes_copied_count=2)
                            _record(samples, transport=transport, case=case.name, metric="download_ms", payload_size=len(blob), result_size=len(downloaded), value_ms=download_ms, bytes_copied_count=2)
                    except Exception:
                        if measured:
                            _record_error(samples, transport=transport, case=case.name, metric="upload_ms", payload_size=len(blob))
                            _record_error(samples, transport=transport, case=case.name, metric="download_ms", payload_size=len(blob))
    finally:
        for client in clients.values():
            client.close()


def _create_service(client: Any, owner: str, name: str):
    return client.create_service_from_bytes(
        owner_client_id=owner,
        service_name=name,
        blob=SERVICE_SOURCE,
        runtime="py3",
        entry_module=f"{name}_module".replace("-", "_"),
        entry_callable="run",
        package_format="py",
        export_mode="function",
        export_methods=["run"],
        worker_count=1,
        expose_http=False,
        heartbeat_timeout_sec=30,
    )


def run_service_bench(samples: List[Sample], node: BenchNode, cases: List[Case], *, warmup: int, repeat: int, include_local_ipc: bool) -> None:
    clients: Dict[str, Any] = {
        "grpc_nodecontrol": NodeControlClient(node.grpc_target, timeout_sec=60.0),
        "http_nodecontrol": HttpNodeControlClient(node.http_control_url, timeout_sec=60.0),
    }
    sessions: Dict[str, Any] = {}
    local_server = None
    try:
        for transport, client in clients.items():
            sessions[transport] = _create_service(client, f"bench-{transport}", f"bench-{transport}")
        if include_local_ipc:
            local_server = start_local_service_ipc(
                node=LocalIpcNode(node.root / "local-ipc-objects"),
                service_name="bench-local-ipc",
            )
            clients["local_ipc"] = LocalServiceClient(service_name="bench-local-ipc", timeout_sec=60.0)
            sessions["local_ipc"] = None

        for transport, client in clients.items():
            session = sessions.get(transport)
            for case in cases:
                iterations = warmup + repeat
                for index in range(iterations):
                    payload = case.payload_factory()
                    measured = index >= warmup
                    payload_size = case.payload_size or _payload_wire_size(payload)
                    try:
                        start = time.perf_counter()
                        raw = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
                        serialization_ms = (time.perf_counter() - start) * 1000.0

                        start = time.perf_counter()
                        if transport == "local_ipc":
                            response = client.call(method="run", payload=payload, timeout_sec=case.timeout_sec, serialization_mode="pickle_stable_v1")
                            data = response.get("data", {})
                        else:
                            response = client.call_service(
                                service_id=session.service_id,
                                method="run",
                                payload=payload,
                                service_token=session.service_token,
                                timeout_sec=case.timeout_sec,
                                serialization_mode="pickle_stable_v1",
                            )
                            data = _decode_service_response(response)
                        total_ms = (time.perf_counter() - start) * 1000.0

                        start = time.perf_counter()
                        pickle.loads(pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL))
                        materialize_ms = (time.perf_counter() - start) * 1000.0
                        result_size = _result_wire_size(data)
                        if measured:
                            _record(samples, transport=transport, case=case.name, metric="serialization_ms", payload_size=payload_size, result_size=result_size, value_ms=serialization_ms, bytes_copied_count=1)
                            _record(samples, transport=transport, case=case.name, metric="total_job_ms", payload_size=payload_size, result_size=result_size, value_ms=total_ms, bytes_copied_count=3)
                            _record(samples, transport=transport, case=case.name, metric="materialize_ms", payload_size=payload_size, result_size=result_size, value_ms=materialize_ms, bytes_copied_count=1)
                    except Exception:
                        if measured:
                            for metric in ("serialization_ms", "total_job_ms", "materialize_ms"):
                                _record_error(samples, transport=transport, case=case.name, metric=metric, payload_size=payload_size)
    finally:
        for transport, session in sessions.items():
            if session is not None:
                try:
                    session.close(reason="benchmark complete")
                except Exception:
                    pass
        for client in clients.values():
            try:
                client.close()
            except Exception:
                pass
        if local_server is not None:
            local_server.close()


def run_taskpool_bench(samples: List[Sample], node: BenchNode, cases: List[Case], *, warmup: int, repeat: int) -> None:
    clients: Dict[str, Any] = {
        "grpc_taskpool": NodeControlClient(node.grpc_target, timeout_sec=60.0),
        "http_taskpool": HttpNodeControlClient(node.http_control_url, timeout_sec=60.0),
    }
    pools: Dict[str, Any] = {}
    try:
        for transport, client in clients.items():
            pools[transport] = client.create_task_pool_from_bytes(
                owner_client_id=f"bench-{transport}",
                pool_name=f"bench-{transport}",
                blob=SERVICE_SOURCE,
                runtime="py3",
                entry_module=f"{transport}_module".replace("-", "_"),
                entry_callable="run",
                package_format="py",
                worker_count=1,
                heartbeat_timeout_sec=30,
            )
        for transport, pool in pools.items():
            for case in cases:
                iterations = warmup + repeat
                for index in range(iterations):
                    payload = case.payload_factory()
                    measured = index >= warmup
                    payload_size = case.payload_size or _payload_wire_size(payload)
                    task_id = f"{transport}-{case.name}-{index}-{time.time_ns()}"
                    task = pb2.TaskSubmitItem(task_id=task_id, timeout_hint_sec=int(case.timeout_sec), transport_payload=pb2.TransportPayload())
                    from pycloud_parallel.controlplane.serialization import encode_transport_payload_bytes

                    task.transport_payload.CopyFrom(encode_transport_payload_bytes(payload, mode="pickle_stable_v1", context="taskpool_session"))
                    try:
                        start_total = time.perf_counter()
                        start = time.perf_counter()
                        pool.submit_tasks([task], job_id=f"job-{task_id}")
                        submit_ms = (time.perf_counter() - start) * 1000.0
                        result = None
                        first_result_ms = math.nan
                        deadline = time.perf_counter() + case.timeout_sec + 5.0
                        while time.perf_counter() < deadline:
                            pulled = pool.pull_results(limit=10, wait_ms=50)
                            for item in pulled.results:
                                if item.task_id == task_id:
                                    result = item
                                    first_result_ms = (time.perf_counter() - start_total) * 1000.0
                                    break
                            if result is not None:
                                break
                        total_ms = (time.perf_counter() - start_total) * 1000.0
                        if result is None:
                            raise TimeoutError("task result not returned")
                        if result.status != pb2.TASK_STATUS_SUCCEEDED:
                            raise RuntimeError(result.error.message or "task failed")
                        data = _decode_task_result(result)
                        result_size = _result_wire_size(data)
                        if measured:
                            _record(samples, transport=transport, case=case.name, metric="submit_ms", payload_size=payload_size, result_size=result_size, value_ms=submit_ms, bytes_copied_count=2)
                            _record(samples, transport=transport, case=case.name, metric="first_result_ms", payload_size=payload_size, result_size=result_size, value_ms=first_result_ms, bytes_copied_count=3)
                            _record(samples, transport=transport, case=case.name, metric="total_job_ms", payload_size=payload_size, result_size=result_size, value_ms=total_ms, bytes_copied_count=3)
                    except Exception:
                        if measured:
                            for metric in ("submit_ms", "first_result_ms", "total_job_ms"):
                                _record_error(samples, transport=transport, case=case.name, metric=metric, payload_size=payload_size)
    finally:
        for pool in pools.values():
            try:
                pool.close(reason="benchmark complete")
            except Exception:
                pass
        for client in clients.values():
            try:
                client.close()
            except Exception:
                pass


def write_csv(path: Path, samples: List[Sample]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=[
                "transport",
                "case",
                "metric",
                "payload_size",
                "result_size",
                "p50_ms",
                "p95_ms",
                "mean_ms",
                "error_count",
                "bytes_copied_count",
                "sample_count",
            ],
        )
        writer.writeheader()
        for sample in sorted(samples, key=lambda item: (item.transport, item.case, item.metric)):
            writer.writerow(
                {
                    "transport": sample.transport,
                    "case": sample.case,
                    "metric": sample.metric,
                    "payload_size": sample.payload_size,
                    "result_size": sample.result_size,
                    "p50_ms": f"{_percentile(sample.values_ms, 0.50):.3f}" if sample.values_ms else "",
                    "p95_ms": f"{_percentile(sample.values_ms, 0.95):.3f}" if sample.values_ms else "",
                    "mean_ms": f"{statistics.fmean(sample.values_ms):.3f}" if sample.values_ms else "",
                    "error_count": sample.error_count,
                    "bytes_copied_count": sample.bytes_copied_count,
                    "sample_count": len(sample.values_ms),
                }
            )


def write_report(path: Path, samples: List[Sample], csv_path: Path) -> None:
    total_rows = len(samples)
    errors = sum(item.error_count for item in samples)
    total_job = [item for item in samples if item.metric == "total_job_ms" and item.values_ms]
    fastest = sorted(total_job, key=lambda item: _percentile(item.values_ms, 0.50))[:8]
    upload = [item for item in samples if item.metric == "upload_ms" and item.values_ms]
    lines = [
        "# Transport Benchmark Report",
        "",
        f"- CSV: `{csv_path}`",
        f"- Rows: {total_rows}",
        f"- Errors: {errors}",
        "",
        "## Fastest total job p50",
    ]
    if fastest:
        for item in fastest:
            lines.append(f"- `{item.transport}` / `{item.case}`: p50 {_percentile(item.values_ms, 0.50):.3f} ms, p95 {_percentile(item.values_ms, 0.95):.3f} ms")
    else:
        lines.append("- No total job samples.")
    lines.extend(["", "## Object upload p50"])
    if upload:
        for item in sorted(upload, key=lambda entry: (entry.case, entry.transport)):
            lines.append(f"- `{item.transport}` / `{item.case}`: p50 {_percentile(item.values_ms, 0.50):.3f} ms")
    else:
        lines.append("- No object upload samples.")
    lines.extend(
        [
            "",
            "## Notes",
            "- HTTP NodeControl currently pays JSON/base64/protobuf JSON conversion overhead, so small control calls may lag gRPC.",
            "- HTTP object upload/download is the main apples-to-apples DataRef comparison against gRPC object streaming.",
            "- `local_ipc` measures local service IPC calls, not remote NodeControl create/submit control-plane work.",
            "- `bytes_copied_count` is an estimate from the benchmark harness, useful for comparing direction rather than exact allocator copies.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark PyCloud gRPC, HTTP, HTTP object, and local IPC transports.")
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--repeat", type=int, default=10)
    parser.add_argument("--full", action="store_true", help="include 8 MB and 5 second cases")
    parser.add_argument("--optional", action="store_true", help="include pandas DataFrame and numpy ndarray cases when installed")
    parser.add_argument("--skip-taskpool", action="store_true")
    parser.add_argument("--skip-service", action="store_true")
    parser.add_argument("--skip-object", action="store_true")
    parser.add_argument("--skip-local-ipc", action="store_true")
    parser.add_argument("--output-dir", default=str(ROOT / "benchmark_results"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("PYCLOUD_LOCAL_IPC_DIR", str(output_dir / "local-ipc-registry"))
    gc.disable()
    samples: List[Sample] = []
    cases = make_cases(full=bool(args.full), include_optional=bool(args.optional))
    with tempfile.TemporaryDirectory(prefix="pycloud-transport-bench-") as tmp:
        node = BenchNode(Path(tmp))
        node.start()
        try:
            if not args.skip_object:
                run_object_bench(samples, node, cases, warmup=max(0, args.warmup), repeat=max(1, args.repeat))
            if not args.skip_service:
                run_service_bench(
                    samples,
                    node,
                    cases,
                    warmup=max(0, args.warmup),
                    repeat=max(1, args.repeat),
                    include_local_ipc=not args.skip_local_ipc,
                )
            if not args.skip_taskpool:
                run_taskpool_bench(samples, node, cases, warmup=max(0, args.warmup), repeat=max(1, args.repeat))
        finally:
            node.close()
    gc.enable()
    stamp = time.strftime("%Y%m%d-%H%M%S")
    csv_path = output_dir / f"transport_benchmark_{stamp}.csv"
    report_path = output_dir / f"transport_benchmark_{stamp}.md"
    write_csv(csv_path, samples)
    write_report(report_path, samples, csv_path)
    print(json.dumps({"csv": str(csv_path), "report": str(report_path), "rows": len(samples)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
