from __future__ import annotations

"""Cross-platform local service manager CLI for PyCloud."""

import argparse
import contextlib
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Dict, Iterable, List, Tuple
from urllib.request import urlopen


def _runtime_root(explicit: str = "") -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()
    env_root = str(os.environ.get("PYCLOUD_HOME", "") or "").strip()
    if env_root:
        return Path(env_root).expanduser().resolve()
    return Path.cwd().resolve()


def _default_node_worker_capacity() -> int:
    cpu_count = os.cpu_count() or 1
    return max(1, cpu_count // 2)


def _logs_dir(root: Path) -> Path:
    return root / "logs"


def _pids_dir(root: Path) -> Path:
    return root / "pids"


def _pid_file(root: Path, name: str) -> Path:
    return _pids_dir(root) / f"{name}.pid"


def _log(label: str, message: str) -> None:
    print(f"[{label}] {time.strftime('%H:%M:%S')} {message}")


def _is_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}"],
            capture_output=True,
            text=True,
            check=False,
        )
        return str(pid) in (result.stdout or "")
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _read_pid(path: Path) -> int:
    try:
        return int(path.read_text(encoding="utf-8").strip() or "0")
    except Exception:
        return 0


def _write_pid(path: Path, pid: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{int(pid)}\n", encoding="utf-8")


def _remove_pid(path: Path) -> None:
    with contextlib.suppress(FileNotFoundError):
        path.unlink()


def _terminate_pid(pid: int, *, force: bool = False) -> None:
    if not _is_pid_running(pid):
        return
    if os.name == "nt":
        cmd = ["taskkill", "/PID", str(pid)]
        if force:
            cmd.insert(1, "/F")
        subprocess.run(cmd, check=False, capture_output=True, text=True)
        return
    sig = signal.SIGKILL if force else signal.SIGTERM
    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, sig)


def _stop_named_process(root: Path, name: str) -> None:
    pid_path = _pid_file(root, name)
    pid = _read_pid(pid_path)
    if pid <= 0:
        _remove_pid(pid_path)
        return
    _log("INFO", f"Stopping {name} (PID: {pid})...")
    _terminate_pid(pid, force=False)
    deadline = time.time() + 3.0
    while time.time() < deadline:
        if not _is_pid_running(pid):
            break
        time.sleep(0.2)
    if _is_pid_running(pid):
        _terminate_pid(pid, force=True)
    _remove_pid(pid_path)


def _wait_controlplane_ready(port: int, timeout_sec: float) -> bool:
    deadline = time.time() + max(0.1, float(timeout_sec))
    url = f"http://127.0.0.1:{int(port)}/nodes?healthy_only=false&limit=1"
    while time.time() < deadline:
        try:
            with urlopen(url, timeout=1.0) as resp:
                data = json.loads(resp.read().decode("utf-8") or "{}")
            if isinstance(data, dict) and data.get("ok") is True:
                return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def _wait_node_registered(infocenter_target: str, node_id: str, timeout_sec: float) -> bool:
    target = str(infocenter_target or "").strip()
    if not target.startswith(("http://", "https://")):
        target = f"http://{target}"
    url = f"{target.rstrip('/')}/nodes?healthy_only=false&limit=500"
    deadline = time.time() + max(0.1, float(timeout_sec))
    while time.time() < deadline:
        try:
            with urlopen(url, timeout=1.0) as resp:
                data = json.loads(resp.read().decode("utf-8") or "{}")
            nodes = data.get("nodes") or []
            if any(str(item.get("node_id", "")) == node_id for item in nodes if isinstance(item, dict)):
                return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def _server_command(*args: str) -> List[str]:
    return [sys.executable, "-m", "pycloud_parallel.controlplane.server", *args]


def _spawn_server(root: Path, log_path: Path, args: Iterable[str]) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("ab") as fp:
        proc = subprocess.Popen(
            _server_command(*args),
            stdout=fp,
            stderr=subprocess.STDOUT,
            cwd=str(root),
            close_fds=(os.name != "nt"),
        )
    return int(proc.pid)


def _start_controlplane(root: Path, port: int) -> None:
    _log("INFO", f"Starting ControlPlane on port {port}...")
    pid = _spawn_server(
        root,
        _logs_dir(root) / "controlplane.log",
        ["--role", "controlplane", "--bind", f"0.0.0.0:{int(port)}", "--log-level", "INFO"],
    )
    if not _wait_controlplane_ready(port, 15.0):
        _remove_pid(_pid_file(root, "controlplane"))
        raise RuntimeError("ControlPlane failed to become ready")
    _write_pid(_pid_file(root, "controlplane"), pid)
    _log("OK", f"ControlPlane started (PID: {pid}, Port: {port})")


def _start_node(root: Path, name: str, port: int, http_port: int, infocenter_target: str, worker_capacity: int) -> None:
    _log("INFO", f"Starting {name} on port {port} (HTTP: {http_port}, workers: {worker_capacity})...")
    pid = _spawn_server(
        root,
        _logs_dir(root) / f"{name}.log",
        [
            "--role",
            "nodecontrol",
            "--bind",
            f"0.0.0.0:{int(port)}",
            "--node-id",
            name,
            "--worker-capacity",
            str(int(worker_capacity)),
            "--queue-capacity",
            "1000",
            "--service-http-bind",
            f"127.0.0.1:{int(http_port)}",
            "--infocenter-addr",
            infocenter_target,
            "--advertise-addr",
            f"127.0.0.1:{int(port)}",
            "--node-tags",
            "compute",
            "--log-level",
            "INFO",
        ],
    )
    if not _wait_node_registered(infocenter_target, name, 15.0):
        _remove_pid(_pid_file(root, name))
        raise RuntimeError(f"{name} failed to register to InfoCenter")
    _write_pid(_pid_file(root, name), pid)
    _log("OK", f"{name} started (PID: {pid}, Port: {port}, HTTP: {http_port}, workers: {worker_capacity})")


def _query_loaded_services(infocenter_port: int) -> List[str]:
    target = f"127.0.0.1:{int(infocenter_port)}"
    try:
        from pycloud_parallel.controlplane.client import InfoCenterClient

        with InfoCenterClient(target, timeout_sec=3) as client:
            with contextlib.redirect_stdout(io.StringIO()):
                nodes = list(client.list_nodes(healthy_only=False, limit=500))
                routes = list(client.list_service_routes(healthy_only=False, limit=5000))
    except Exception as exc:
        return [f"  (query failed: {exc})"]

    by_node: Dict[str, List[str]] = {}
    for route in routes:
        node_id = str(getattr(route, "node_id", "") or "").strip()
        service_name = str(getattr(route, "service_name", "") or "").strip()
        if node_id and service_name:
            by_node.setdefault(node_id, []).append(service_name)

    if not nodes:
        return ["  (no nodes)"]
    lines: List[str] = []
    for node in sorted(nodes, key=lambda x: getattr(x, "node_id", "")):
        node_id = getattr(node, "node_id", "")
        pyver = (getattr(node, "python_version", "") or "").strip() or "unknown"
        names = sorted(set(by_node.get(node_id, [])))
        if names:
            lines.append(f"  - {node_id} [{pyver}]: {', '.join(names)}")
        else:
            lines.append(f"  - {node_id} [{pyver}]: (none)")
    return lines


def _cmd_start(args: argparse.Namespace) -> int:
    root = _runtime_root(args.runtime_root)
    _logs_dir(root).mkdir(parents=True, exist_ok=True)
    _pids_dir(root).mkdir(parents=True, exist_ok=True)

    _log("INFO", "Stopping existing services...")
    for name in ("node-1", "node-2", "controlplane"):
        _stop_named_process(root, name)
    time.sleep(1.0)

    _start_controlplane(root, args.controlplane_port)
    time.sleep(2.0)
    worker_capacity = int(args.node_worker_capacity or _default_node_worker_capacity())
    infocenter_target = f"127.0.0.1:{int(args.controlplane_port)}"
    _start_node(root, "node-1", args.node1_port, args.node1_http, infocenter_target, worker_capacity)
    _start_node(root, "node-2", args.node2_port, args.node2_http, infocenter_target, worker_capacity)

    print("============================================")
    print("  All Services Started!")
    print("============================================")
    print()
    print(f"  ControlPlane: 127.0.0.1:{int(args.controlplane_port)}")
    print(f"  Node-1:      127.0.0.1:{int(args.node1_port)} (HTTP: {int(args.node1_http)})")
    print(f"  Node-2:      127.0.0.1:{int(args.node2_port)} (HTTP: {int(args.node2_http)})")
    print(f"  Worker cap:  {worker_capacity} per node")
    print(f"  Logs:        {_logs_dir(root)}")
    print(f"  PIDs:        {_pids_dir(root)}")
    return 0


def _cmd_stop(args: argparse.Namespace) -> int:
    root = _runtime_root(args.runtime_root)
    for name in ("node-1", "node-2", "controlplane"):
        _stop_named_process(root, name)
    _log("OK", "All services stopped")
    return 0


def _cmd_restart(args: argparse.Namespace) -> int:
    stop_code = _cmd_stop(args)
    if stop_code != 0:
        return stop_code
    time.sleep(2.0)
    return _cmd_start(args)


def _cmd_status(args: argparse.Namespace) -> int:
    root = _runtime_root(args.runtime_root)
    print("============================================")
    print("  Service Status")
    print("============================================")
    print()
    status_code = 0
    for name in ("controlplane", "node-1", "node-2"):
        pid = _read_pid(_pid_file(root, name))
        if pid > 0 and _is_pid_running(pid):
            print(f"  * {name} (PID: {pid}) - RUNNING")
        else:
            print(f"  - {name} - NOT STARTED")
            status_code = 1
    print()
    print("  Loaded Services By Node")
    print("  ------------------------------------------")
    for line in _query_loaded_services(args.controlplane_port):
        print(line)
    return status_code


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="PyCloud local service manager")
    parser.add_argument("--runtime-root", default="", help="base directory for logs and pid files (default: cwd or PYCLOUD_HOME)")
    parser.add_argument("--controlplane-port", type=int, default=50051)
    parser.add_argument("--node1-port", type=int, default=50061)
    parser.add_argument("--node1-http", type=int, default=18081)
    parser.add_argument("--node2-port", type=int, default=50062)
    parser.add_argument("--node2-http", type=int, default=18082)
    parser.add_argument("--node-worker-capacity", type=int, default=0, help="worker capacity per nodecontrol; default auto-calculated from CPU")

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("start", help="start controlplane and two nodecontrol processes")
    subparsers.add_parser("stop", help="stop local services started by pycloudctl")
    subparsers.add_parser("restart", help="restart local services")
    subparsers.add_parser("status", help="show local service status")
    return parser


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "start":
        return _cmd_start(args)
    if args.command == "stop":
        return _cmd_stop(args)
    if args.command == "restart":
        return _cmd_restart(args)
    if args.command == "status":
        return _cmd_status(args)
    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
