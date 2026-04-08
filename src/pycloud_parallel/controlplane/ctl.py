from __future__ import annotations

"""Cross-platform local service manager CLI for PyCloud."""

import argparse
import contextlib
from datetime import datetime, timedelta, timezone
import io
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
from typing import Dict, Iterable, List, Tuple
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

from pycloud_parallel.controlplane.config import (
    NODE_MAX_WORKERS,
    NODE_QUEUE_CAPACITY,
    NODE_WORKER_CAPACITY,
    SERVICE_DEFAULT_WORKERS,
    SERVICE_HEARTBEAT_TIMEOUT_SEC,
)
from pycloud_parallel.controlplane.object_ref import normalize_object_id


def _runtime_root(explicit: str = "") -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()
    env_root = str(os.environ.get("PYCLOUD_HOME", "") or "").strip()
    if env_root:
        return Path(env_root).expanduser().resolve()
    return Path.cwd().resolve()


def _default_node_worker_capacity() -> int:
    env_value = str(os.environ.get("PYCLOUD_NODE_WORKER_CAPACITY", "") or "").strip()
    if env_value:
        return NODE_WORKER_CAPACITY
    cpu_count = os.cpu_count() or 1
    return max(1, cpu_count // 2)


def _logs_dir(root: Path) -> Path:
    return root / "logs"


def _pids_dir(root: Path) -> Path:
    return root / "pids"


def _pid_file(root: Path, name: str) -> Path:
    return _pids_dir(root) / f"{name}.pid"


def _split_host_port(bind: str) -> Tuple[str, int]:
    text = str(bind or "").strip()
    if ":" not in text:
        raise ValueError("bind must be host:port")
    host, port = text.rsplit(":", 1)
    return host.strip(), int(port)


def _probe_host(host: str) -> str:
    text = str(host or "").strip()
    if text in {"", "0.0.0.0", "::", "[::]"}:
        return "127.0.0.1"
    return text


def _normalize_managed_name(value: str) -> str:
    name = str(value or "").strip()
    if not name:
        raise argparse.ArgumentTypeError("name must not be empty")
    if "/" in name or "\\" in name:
        raise argparse.ArgumentTypeError("name must not contain path separators")
    return name


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


def _assert_bind_available(bind: str) -> None:
    host, port = _split_host_port(bind)
    bind_host = host.strip()
    family = socket.AF_INET6 if ":" in bind_host and bind_host not in {"0.0.0.0", ""} else socket.AF_INET
    if family == socket.AF_INET and bind_host in {"", "::", "[::]"}:
        bind_host = "0.0.0.0"
    if family == socket.AF_INET6 and bind_host in {"", "0.0.0.0"}:
        bind_host = "::"
    if bind_host.startswith("[") and bind_host.endswith("]"):
        bind_host = bind_host[1:-1]
    sock = socket.socket(family, socket.SOCK_STREAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((bind_host, int(port)))
    except OSError as exc:
        raise RuntimeError(f"bind address is already in use or unavailable: {bind}") from exc
    finally:
        sock.close()


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


def _pid_names(root: Path) -> List[str]:
    pids_dir = _pids_dir(root)
    if not pids_dir.exists():
        return []
    return sorted(path.stem for path in pids_dir.glob("*.pid") if path.is_file())


def _process_sort_key(name: str) -> Tuple[int, str]:
    if name.startswith("node-"):
        return (0, name)
    if name == "gateway":
        return (1, name)
    if name == "controlplane":
        return (2, name)
    if name == "infocenter":
        return (3, name)
    return (4, name)


def _managed_process_names(root: Path) -> List[str]:
    names = {"controlplane", "node-1", "node-2"}
    names.update(_pid_names(root))
    return sorted(names, key=_process_sort_key)


def _stop_all_managed_processes(root: Path) -> None:
    for name in _managed_process_names(root):
        _stop_named_process(root, name)


def _default_named_ports(args: argparse.Namespace) -> Dict[str, int]:
    return {
        "controlplane": int(args.controlplane_port),
        "node-1-grpc": int(args.node1_port),
        "node-1-http": int(args.node1_http),
        "node-2-grpc": int(args.node2_port),
        "node-2-http": int(args.node2_http),
    }


def _parse_ports_csv(value: str) -> List[int]:
    ports: List[int] = []
    for item in str(value or "").split(","):
        text = item.strip()
        if not text:
            continue
        port = int(text)
        if port <= 0 or port > 65535:
            raise ValueError(f"invalid port: {text}")
        ports.append(port)
    return ports


def _collect_scan_ports(args: argparse.Namespace) -> List[int]:
    if getattr(args, "ports", ""):
        values = _parse_ports_csv(str(args.ports))
        return list(dict.fromkeys(values))
    return list(dict.fromkeys(_default_named_ports(args).values()))


def _listener_pids_for_port(port: int) -> List[int]:
    if os.name == "nt":
        result = subprocess.run(
            ["netstat", "-ano", "-p", "tcp"],
            capture_output=True,
            text=True,
            check=False,
        )
        pids: List[int] = []
        for raw in (result.stdout or "").splitlines():
            line = raw.strip()
            if not line or "LISTENING" not in line.upper():
                continue
            parts = line.split()
            if len(parts) < 5:
                continue
            local_addr = parts[1]
            state = parts[3].upper()
            pid_text = parts[4]
            if state != "LISTENING":
                continue
            if not local_addr.endswith(f":{int(port)}"):
                continue
            try:
                pid = int(pid_text)
            except Exception:
                continue
            if pid > 0:
                pids.append(pid)
        return sorted(set(pids))

    result = subprocess.run(
        ["lsof", "-nP", f"-iTCP:{int(port)}", "-sTCP:LISTEN", "-t"],
        capture_output=True,
        text=True,
        check=False,
    )
    pids = []
    for raw in (result.stdout or "").splitlines():
        text = raw.strip()
        if not text:
            continue
        try:
            pid = int(text)
        except Exception:
            continue
        if pid > 0:
            pids.append(pid)
    return sorted(set(pids))


def _command_for_pid(pid: int) -> str:
    if pid <= 0:
        return ""
    if os.name == "nt":
        ps_result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                f"(Get-CimInstance Win32_Process -Filter \"ProcessId = {int(pid)}\").CommandLine",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        text = str(ps_result.stdout or "").strip()
        if text:
            return text
        tasklist_result = subprocess.run(
            ["tasklist", "/FO", "CSV", "/NH", "/FI", f"PID eq {int(pid)}"],
            capture_output=True,
            text=True,
            check=False,
        )
        return str(tasklist_result.stdout or "").strip()
    result = subprocess.run(
        ["ps", "-p", str(int(pid)), "-o", "command="],
        capture_output=True,
        text=True,
        check=False,
    )
    return str(result.stdout or "").strip()


def _looks_like_pycloud_process(command: str) -> bool:
    text = str(command or "").strip().lower()
    if not text:
        return False
    needles = (
        "pycloud_parallel.controlplane.server",
        "pycloud-control",
        "pycloudctl",
        "start_services.sh",
        "start_services.bat",
    )
    return any(needle in text for needle in needles)


def _role_from_command(command: str) -> str:
    match = re.search(r"--role\s+([A-Za-z0-9_-]+)", str(command or ""))
    return str(match.group(1) if match else "").strip()


def _node_name_from_command(command: str) -> str:
    match = re.search(r"--node-id\s+([^\s]+)", str(command or ""))
    return str(match.group(1) if match else "").strip()


def _inspect_listening_ports(ports: Iterable[int]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for port in ports:
        pids = _listener_pids_for_port(int(port))
        if not pids:
            rows.append({"port": int(port), "pid": 0, "command": "", "matches_pycloud": False, "role": "", "node_name": ""})
            continue
        for pid in pids:
            command = _command_for_pid(int(pid))
            rows.append(
                {
                    "port": int(port),
                    "pid": int(pid),
                    "command": command,
                    "matches_pycloud": _looks_like_pycloud_process(command),
                    "role": _role_from_command(command),
                    "node_name": _node_name_from_command(command),
                }
            )
    return rows


def _kill_scanned_port_processes(*, target: str, ports: Iterable[int]) -> List[Dict[str, object]]:
    killed: List[Dict[str, object]] = []
    seen_pids: set[int] = set()
    for row in _inspect_listening_ports(ports):
        pid = int(row.get("pid", 0) or 0)
        if pid <= 0 or pid in seen_pids:
            continue
        seen_pids.add(pid)
        if not bool(row.get("matches_pycloud", False)):
            continue
        node_name = str(row.get("node_name", "") or "").strip()
        role = str(row.get("role", "") or "").strip()
        if role == "nodecontrol" and node_name:
            _best_effort_mark_node_lost(target, node_name)
        _log("INFO", f"Stopping scanned listener PID {pid} on port {int(row['port'])}...")
        _terminate_pid(pid, force=False)
        deadline = time.time() + 3.0
        while time.time() < deadline:
            if not _is_pid_running(pid):
                break
            time.sleep(0.2)
        if _is_pid_running(pid):
            _terminate_pid(pid, force=True)
        killed.append(dict(row))
    return killed


def _wait_http_json_ok(host: str, port: int, timeout_sec: float, *, path: str) -> bool:
    deadline = time.time() + max(0.1, float(timeout_sec))
    url = f"http://{_probe_host(host)}:{int(port)}{path}"
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


def _wait_http_ready(bind: str, timeout_sec: float, *, path: str = "/") -> bool:
    host, port = _split_host_port(bind)
    deadline = time.time() + max(0.1, float(timeout_sec))
    url = f"http://{_probe_host(host)}:{int(port)}{path}"
    while time.time() < deadline:
        try:
            with urlopen(url, timeout=1.0):
                return True
        except HTTPError:
            return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


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


def _normalize_http_target(target: str) -> str:
    text = str(target or "").strip()
    if not text:
        return ""
    if text.startswith(("http://", "https://")):
        return text.rstrip("/")
    return f"http://{text}".rstrip("/")


def _best_effort_mark_node_lost(target: str, node_name: str) -> bool:
    normalized_target = _normalize_http_target(target)
    if not normalized_target or not str(node_name or "").strip():
        return False
    req = Request(
        url=f"{normalized_target}/ops/nodes/{quote(str(node_name).strip(), safe='')}/mark-lost",
        method="POST",
        data=b"",
    )
    try:
        with urlopen(req, timeout=2.0) as resp:
            data = json.loads(resp.read().decode("utf-8") or "{}")
        return isinstance(data, dict) and data.get("ok") is True
    except Exception:
        return False


def _server_command(*args: str) -> List[str]:
    return [sys.executable, "-m", "pycloud_parallel.controlplane.server", *args]


def _parse_env_overrides(items: Iterable[str]) -> Dict[str, str]:
    env: Dict[str, str] = {}
    for item in items:
        text = str(item or "").strip()
        if not text:
            continue
        if "=" not in text:
            raise ValueError(f"--env must be KEY=VALUE, got {text!r}")
        key, value = text.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"--env key must not be empty, got {text!r}")
        env[key] = value
    return env


def _env_override_int(extra_env: Dict[str, str], key: str, default: int) -> int:
    raw = str((extra_env or {}).get(key, "") or "").strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"--env {key} must be int, got {raw!r}") from exc


def _spawn_server(root: Path, log_path: Path, args: Iterable[str], *, extra_env: Dict[str, str] | None = None) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    if extra_env:
        env.update({str(k): str(v) for k, v in extra_env.items()})
    if os.name == "nt":
        creationflags = int(getattr(subprocess, "CREATE_NEW_CONSOLE", 0) or 0)
        proc = subprocess.Popen(
            _server_command(*args),
            stdout=None,
            stderr=None,
            cwd=str(root),
            env=env,
            close_fds=False,
            creationflags=creationflags,
        )
    else:
        with log_path.open("ab") as fp:
            proc = subprocess.Popen(
                _server_command(*args),
                stdout=fp,
                stderr=subprocess.STDOUT,
                cwd=str(root),
                env=env,
                close_fds=True,
            )
    return int(proc.pid)


def _wait_ready_with_pid(pid: int, timeout_sec: float, checker) -> bool:
    deadline = time.time() + max(0.1, float(timeout_sec))
    while time.time() < deadline:
        if checker():
            return True
        if pid > 0 and not _is_pid_running(pid):
            return False
        time.sleep(0.2)
    return False


def _ensure_runtime_dirs(root: Path) -> None:
    _logs_dir(root).mkdir(parents=True, exist_ok=True)
    _pids_dir(root).mkdir(parents=True, exist_ok=True)


def _start_controlplane(root: Path, port: int, *, extra_env: Dict[str, str] | None = None) -> None:
    _assert_bind_available(f"0.0.0.0:{int(port)}")
    _log("INFO", f"Starting ControlPlane on port {port}...")
    pid = _spawn_server(
        root,
        _logs_dir(root) / "controlplane.log",
        ["--role", "controlplane", "--bind", f"0.0.0.0:{int(port)}", "--log-level", "INFO"],
        extra_env=extra_env,
    )
    if not _wait_ready_with_pid(pid, 15.0, lambda: _wait_controlplane_ready(port, 0.2)):
        _remove_pid(_pid_file(root, "controlplane"))
        raise RuntimeError("ControlPlane failed to become ready")
    _write_pid(_pid_file(root, "controlplane"), pid)
    _log("OK", f"ControlPlane started (PID: {pid}, Port: {port})")


def _start_infocenter(root: Path, *, bind: str, extra_env: Dict[str, str] | None = None) -> None:
    host, port = _split_host_port(bind)
    _assert_bind_available(bind)
    _log("INFO", f"Starting InfoCenter on {bind}...")
    pid = _spawn_server(
        root,
        _logs_dir(root) / "infocenter.log",
        ["--role", "infocenter", "--bind", bind, "--log-level", "INFO"],
        extra_env=extra_env,
    )
    if not _wait_ready_with_pid(pid, 15.0, lambda: _wait_http_json_ok(host, port, 0.2, path="/nodes?healthy_only=false&limit=1")):
        _remove_pid(_pid_file(root, "infocenter"))
        raise RuntimeError("InfoCenter failed to become ready")
    _write_pid(_pid_file(root, "infocenter"), pid)
    _log("OK", f"InfoCenter started (PID: {pid}, Bind: {bind})")


def _start_standalone_controlplane(
    root: Path,
    *,
    bind: str,
    gateway_refresh_interval_sec: float,
    gateway_failure_threshold: int,
    gateway_open_sec: float,
    extra_env: Dict[str, str] | None = None,
) -> None:
    host, port = _split_host_port(bind)
    _assert_bind_available(bind)
    _log("INFO", f"Starting ControlPlane on {bind}...")
    pid = _spawn_server(
        root,
        _logs_dir(root) / "controlplane.log",
        [
            "--role",
            "controlplane",
            "--bind",
            bind,
            "--gateway-refresh-interval-sec",
            f"{float(gateway_refresh_interval_sec):.3f}",
            "--gateway-failure-threshold",
            str(int(gateway_failure_threshold)),
            "--gateway-open-sec",
            f"{float(gateway_open_sec):.3f}",
            "--log-level",
            "INFO",
        ],
        extra_env=extra_env,
    )
    if not _wait_ready_with_pid(pid, 15.0, lambda: _wait_http_json_ok(host, port, 0.2, path="/nodes?healthy_only=false&limit=1")):
        _remove_pid(_pid_file(root, "controlplane"))
        raise RuntimeError("ControlPlane failed to become ready")
    _write_pid(_pid_file(root, "controlplane"), pid)
    _log("OK", f"ControlPlane started (PID: {pid}, Bind: {bind})")


def _start_gateway(
    root: Path,
    *,
    bind: str,
    infocenter_addr: str,
    gateway_refresh_interval_sec: float,
    gateway_failure_threshold: int,
    gateway_open_sec: float,
    extra_env: Dict[str, str] | None = None,
) -> None:
    _assert_bind_available(bind)
    _log("INFO", f"Starting Gateway on {bind} (InfoCenter: {infocenter_addr})...")
    pid = _spawn_server(
        root,
        _logs_dir(root) / "gateway.log",
        [
            "--role",
            "gateway",
            "--bind",
            bind,
            "--infocenter-addr",
            infocenter_addr,
            "--gateway-refresh-interval-sec",
            f"{float(gateway_refresh_interval_sec):.3f}",
            "--gateway-failure-threshold",
            str(int(gateway_failure_threshold)),
            "--gateway-open-sec",
            f"{float(gateway_open_sec):.3f}",
            "--log-level",
            "INFO",
        ],
        extra_env=extra_env,
    )
    if not _wait_ready_with_pid(pid, 15.0, lambda: _wait_http_ready(bind, 0.2, path="/svc/__pycloudctl_probe__/status")):
        _remove_pid(_pid_file(root, "gateway"))
        raise RuntimeError("Gateway failed to become ready")
    _write_pid(_pid_file(root, "gateway"), pid)
    _log("OK", f"Gateway started (PID: {pid}, Bind: {bind}, InfoCenter: {infocenter_addr})")


def _start_node(
    root: Path,
    name: str,
    port: int,
    http_port: int,
    infocenter_target: str,
    worker_capacity: int,
    *,
    queue_capacity: int = NODE_QUEUE_CAPACITY,
    max_workers: int = NODE_MAX_WORKERS,
    service_default_workers: int = SERVICE_DEFAULT_WORKERS,
    service_heartbeat_timeout_sec: int = SERVICE_HEARTBEAT_TIMEOUT_SEC,
    extra_env: Dict[str, str] | None = None,
) -> None:
    _assert_bind_available(f"0.0.0.0:{int(port)}")
    _assert_bind_available(f"127.0.0.1:{int(http_port)}")
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
            str(int(queue_capacity)),
            "--max-workers",
            str(int(max_workers)),
            "--service-default-workers",
            str(int(service_default_workers)),
            "--service-heartbeat-timeout-sec",
            str(int(service_heartbeat_timeout_sec)),
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
        extra_env=extra_env,
    )
    if not _wait_ready_with_pid(pid, 15.0, lambda: _wait_node_registered(infocenter_target, name, 0.2)):
        _remove_pid(_pid_file(root, name))
        raise RuntimeError(f"{name} failed to register to InfoCenter")
    _write_pid(_pid_file(root, name), pid)
    _log("OK", f"{name} started (PID: {pid}, Port: {port}, HTTP: {http_port}, workers: {worker_capacity})")


def _start_standalone_node(
    root: Path,
    *,
    node_id: str,
    bind: str,
    service_http_bind: str,
    infocenter_addr: str,
    advertise_addr: str,
    worker_capacity: int,
    queue_capacity: int,
    max_workers: int,
    service_default_workers: int,
    service_heartbeat_timeout_sec: int,
    node_tags: str,
    node_version: str,
    extra_env: Dict[str, str] | None = None,
) -> None:
    _assert_bind_available(bind)
    _assert_bind_available(service_http_bind)
    _log(
        "INFO",
        f"Starting {node_id} bind={bind} service_http={service_http_bind} "
        f"workers={worker_capacity} queue={queue_capacity} infocenter={infocenter_addr or '(none)'}...",
    )
    args = [
        "--role",
        "nodecontrol",
        "--bind",
        bind,
        "--node-id",
        node_id,
        "--worker-capacity",
        str(int(worker_capacity)),
        "--queue-capacity",
        str(int(queue_capacity)),
        "--max-workers",
        str(int(max_workers)),
        "--service-default-workers",
        str(int(service_default_workers)),
        "--service-heartbeat-timeout-sec",
        str(int(service_heartbeat_timeout_sec)),
        "--service-http-bind",
        service_http_bind,
        "--node-tags",
        node_tags,
        "--node-version",
        node_version,
        "--log-level",
        "INFO",
    ]
    if infocenter_addr:
        args.extend(["--infocenter-addr", infocenter_addr])
    if advertise_addr:
        args.extend(["--advertise-addr", advertise_addr])
    pid = _spawn_server(root, _logs_dir(root) / f"{node_id}.log", args, extra_env=extra_env)
    if not _wait_ready_with_pid(pid, 15.0, lambda: _wait_http_ready(service_http_bind, 0.2)):
        _remove_pid(_pid_file(root, node_id))
        raise RuntimeError(f"{node_id} service HTTP failed to become ready")
    if infocenter_addr and not _wait_ready_with_pid(pid, 15.0, lambda: _wait_node_registered(infocenter_addr, node_id, 0.2)):
        _remove_pid(_pid_file(root, node_id))
        raise RuntimeError(f"{node_id} failed to register to InfoCenter")
    _write_pid(_pid_file(root, node_id), pid)
    _log(
        "OK",
        f"{node_id} started (PID: {pid}, Bind: {bind}, HTTP: {service_http_bind}, workers: {worker_capacity})",
    )


def _query_loaded_services(target: str) -> List[str]:
    try:
        from pycloud_parallel.controlplane.client import InfoCenterClient

        with InfoCenterClient(target, timeout_sec=3) as client:
            with contextlib.redirect_stdout(io.StringIO()):
                nodes = list(client.list_nodes(healthy_only=False, limit=500))
    except Exception as exc:
        return [f"  (query failed: {exc})"]

    if not nodes:
        return ["  (no nodes)"]
    lines: List[str] = []
    for node in sorted(nodes, key=lambda x: getattr(x, "node_id", "")):
        node_id = getattr(node, "node_id", "")
        pyver = (getattr(node, "python_version", "") or "").strip() or "unknown"
        services = sorted(
            getattr(node, "services", ()),
            key=lambda item: (getattr(item, "service_name", ""), getattr(item, "service_id", "")),
        )
        if services:
            rendered = ", ".join(
                f"{getattr(svc, 'service_name', '?')}[{getattr(svc, 'alive_workers', 0)}/{getattr(svc, 'worker_count', 0)}]"
                for svc in services
            )
            lines.append(f"  - {node_id} [{pyver}]: {rendered}")
        else:
            lines.append(f"  - {node_id} [{pyver}]: (none)")
    return lines


def _cmd_start(args: argparse.Namespace) -> int:
    root = _runtime_root(args.runtime_root)
    _ensure_runtime_dirs(root)
    extra_env = _parse_env_overrides(getattr(args, "env", []) or [])

    _log("INFO", "Stopping existing services...")
    _stop_all_managed_processes(root)
    time.sleep(1.0)

    _start_controlplane(root, args.controlplane_port, extra_env=extra_env)
    time.sleep(2.0)
    worker_capacity = int(args.node_worker_capacity or _env_override_int(extra_env, "PYCLOUD_NODE_WORKER_CAPACITY", _default_node_worker_capacity()))
    queue_capacity = _env_override_int(extra_env, "PYCLOUD_NODE_QUEUE_CAPACITY", NODE_QUEUE_CAPACITY)
    max_workers = _env_override_int(extra_env, "PYCLOUD_NODE_MAX_WORKERS", NODE_MAX_WORKERS)
    service_default_workers = _env_override_int(extra_env, "PYCLOUD_SERVICE_DEFAULT_WORKERS", SERVICE_DEFAULT_WORKERS)
    service_heartbeat_timeout_sec = _env_override_int(
        extra_env,
        "PYCLOUD_SERVICE_HEARTBEAT_TIMEOUT_SEC",
        SERVICE_HEARTBEAT_TIMEOUT_SEC,
    )
    infocenter_target = f"127.0.0.1:{int(args.controlplane_port)}"
    _start_node(
        root,
        "node-1",
        args.node1_port,
        args.node1_http,
        infocenter_target,
        worker_capacity,
        queue_capacity=queue_capacity,
        max_workers=max_workers,
        service_default_workers=service_default_workers,
        service_heartbeat_timeout_sec=service_heartbeat_timeout_sec,
        extra_env=extra_env,
    )
    _start_node(
        root,
        "node-2",
        args.node2_port,
        args.node2_http,
        infocenter_target,
        worker_capacity,
        queue_capacity=queue_capacity,
        max_workers=max_workers,
        service_default_workers=service_default_workers,
        service_heartbeat_timeout_sec=service_heartbeat_timeout_sec,
        extra_env=extra_env,
    )

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
    target = str(getattr(args, "target", "") or f"127.0.0.1:{int(args.controlplane_port)}")
    for name in _managed_process_names(root):
        if name not in {"controlplane", "gateway", "infocenter"}:
            _best_effort_mark_node_lost(target, name)
    _stop_all_managed_processes(root)
    if bool(getattr(args, "scan_ports", False)):
        scanned_ports = _collect_scan_ports(args)
        killed = _kill_scanned_port_processes(target=target, ports=scanned_ports)
        if killed:
            _log("OK", f"Stopped {len(killed)} additional scanned listener process(es)")
    _log("OK", "All services stopped")
    return 0


def _cmd_stop_node(args: argparse.Namespace) -> int:
    root = _runtime_root(args.runtime_root)
    node_name = _normalize_managed_name(args.node_name)
    target = str(getattr(args, "target", "") or f"127.0.0.1:{int(args.controlplane_port)}")
    _best_effort_mark_node_lost(target, node_name)
    _stop_named_process(root, node_name)
    _log("OK", f"{node_name} stopped")
    return 0


def _cmd_start_infocenter(args: argparse.Namespace) -> int:
    root = _runtime_root(args.runtime_root)
    _ensure_runtime_dirs(root)
    _stop_named_process(root, "infocenter")
    _start_infocenter(root, bind=str(args.bind), extra_env=_parse_env_overrides(getattr(args, "env", []) or []))
    return 0


def _cmd_start_gateway(args: argparse.Namespace) -> int:
    root = _runtime_root(args.runtime_root)
    _ensure_runtime_dirs(root)
    _stop_named_process(root, "gateway")
    extra_env = _parse_env_overrides(getattr(args, "env", []) or [])
    _start_gateway(
        root,
        bind=str(args.bind),
        infocenter_addr=str(args.infocenter_addr),
        gateway_refresh_interval_sec=float(args.gateway_refresh_interval_sec),
        gateway_failure_threshold=int(args.gateway_failure_threshold),
        gateway_open_sec=float(args.gateway_open_sec),
        extra_env=extra_env,
    )
    return 0


def _cmd_start_controlplane(args: argparse.Namespace) -> int:
    root = _runtime_root(args.runtime_root)
    _ensure_runtime_dirs(root)
    _stop_named_process(root, "controlplane")
    extra_env = _parse_env_overrides(getattr(args, "env", []) or [])
    _start_standalone_controlplane(
        root,
        bind=str(args.bind),
        gateway_refresh_interval_sec=float(args.gateway_refresh_interval_sec),
        gateway_failure_threshold=int(args.gateway_failure_threshold),
        gateway_open_sec=float(args.gateway_open_sec),
        extra_env=extra_env,
    )
    return 0


def _cmd_start_node(args: argparse.Namespace) -> int:
    root = _runtime_root(args.runtime_root)
    _ensure_runtime_dirs(root)
    extra_env = _parse_env_overrides(getattr(args, "env", []) or [])
    node_id = _normalize_managed_name(args.node_id)
    _stop_named_process(root, node_id)
    worker_capacity = int(args.worker_capacity or _default_node_worker_capacity())
    infocenter_arg = getattr(args, "infocenter_addr", None)
    infocenter_addr = "" if infocenter_arg is None else str(infocenter_arg or "").strip()
    if infocenter_arg is None and getattr(args, "controlplane_port", 0):
        infocenter_addr = f"127.0.0.1:{int(args.controlplane_port)}"
    bind = str(args.bind)
    advertise_addr = str(args.advertise_addr or "").strip()
    if infocenter_addr and not advertise_addr:
        host, port = _split_host_port(bind)
        advertise_addr = f"{_probe_host(host)}:{int(port)}"
    _start_standalone_node(
        root,
        node_id=node_id,
        bind=bind,
        service_http_bind=str(args.service_http_bind),
        infocenter_addr=infocenter_addr,
        advertise_addr=advertise_addr,
        worker_capacity=worker_capacity,
        queue_capacity=int(args.queue_capacity),
        max_workers=int(args.max_workers),
        service_default_workers=int(args.service_default_workers),
        service_heartbeat_timeout_sec=int(args.service_heartbeat_timeout_sec),
        node_tags=str(args.node_tags),
        node_version=str(args.node_version),
        extra_env=extra_env,
    )
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
    for name in _managed_process_names(root):
        pid = _read_pid(_pid_file(root, name))
        if pid > 0 and _is_pid_running(pid):
            print(f"  * {name} (PID: {pid}) - RUNNING")
        else:
            print(f"  - {name} - NOT STARTED")
            status_code = 1
    print()
    print("  Loaded Services By Node")
    print("  ------------------------------------------")
    target = str(getattr(args, "target", "") or f"127.0.0.1:{int(args.controlplane_port)}")
    for line in _query_loaded_services(target):
        print(line)
    return status_code


def _cmd_doctor(args: argparse.Namespace) -> int:
    root = _runtime_root(args.runtime_root)
    target = str(getattr(args, "target", "") or f"127.0.0.1:{int(args.controlplane_port)}")
    named_ports = _default_named_ports(args)
    selected_ports = _collect_scan_ports(args)
    port_rows = _inspect_listening_ports(selected_ports)

    print("============================================")
    print("  PyCloud Doctor")
    print("============================================")
    print()
    print(f"  Runtime Root: {root}")
    print(f"  Query Target: {target}")
    print()
    print("  PID Files")
    print("  ------------------------------------------")
    managed_names = _managed_process_names(root)
    if not managed_names:
        print("  (no managed pid files)")
    else:
        for name in managed_names:
            pid = _read_pid(_pid_file(root, name))
            running = pid > 0 and _is_pid_running(pid)
            if pid > 0:
                print(f"  - {name}: pid={pid} running={'yes' if running else 'no'}")
            else:
                print(f"  - {name}: pid=(missing) running=no")
    print()
    print("  Port Listeners")
    print("  ------------------------------------------")
    if not port_rows:
        print("  (no scanned ports)")
    else:
        label_map = {port: label for label, port in named_ports.items()}
        for row in port_rows:
            port = int(row["port"])
            label = label_map.get(port, "")
            prefix = f"  - {port}"
            if label:
                prefix += f" ({label})"
            pid = int(row.get("pid", 0) or 0)
            if pid <= 0:
                print(f"{prefix}: no listener")
                continue
            command = str(row.get("command", "") or "").strip()
            short_command = command if len(command) <= 120 else f"{command[:117]}..."
            print(
                f"{prefix}: pid={pid} pycloud={'yes' if row.get('matches_pycloud') else 'no'} "
                f"role={row.get('role') or '-'} node={row.get('node_name') or '-'}"
            )
            if short_command:
                print(f"    cmd: {short_command}")
    return 0


def _default_artifact_dir(root: Path) -> Path:
    return root / "code_cache"


def _object_meta_dir(object_dir: Path) -> Path:
    return object_dir / "meta"


def _load_json(path: Path) -> Dict[str, object]:
    try:
        return json.loads(path.read_text(encoding="utf-8") or "{}")
    except Exception:
        return {}


def _collect_current_globals_object_ids(artifact_dir: Path) -> set[str]:
    live: set[str] = set()
    codes_dir = artifact_dir / "codes"
    if not codes_dir.exists():
        return live
    for current_path in codes_dir.glob("*/scopes/*/*/current.json"):
        current = _load_json(current_path)
        globals_digest = str(current.get("globals_digest", "") or "").strip()
        if not globals_digest:
            continue
        scope_dir = current_path.parent
        digest = globals_digest.replace("sha256:", "", 1).strip().lower()
        manifest_path = scope_dir / "manifests" / f"{digest}.json"
        manifest = _load_json(manifest_path)
        values = dict(manifest.get("values") or {})
        for item in values.values():
            if not isinstance(item, dict):
                continue
            value_digest = str(item.get("sha256", "") or "").strip()
            if not value_digest:
                continue
            value_path = scope_dir / "values" / f"{value_digest.replace('sha256:', '', 1).strip().lower()}.json"
            value_payload = _load_json(value_path)

            def _walk(value):
                if isinstance(value, dict):
                    if set(value.keys()) == {"__pycloud_object_ref__"} and isinstance(value.get("__pycloud_object_ref__"), dict):
                        ref = value["__pycloud_object_ref__"]
                        object_id = normalize_object_id(str(ref.get("object_id", "") or ""))
                        if object_id:
                            live.add(object_id)
                        return
                    for child in value.values():
                        _walk(child)
                    return
                if isinstance(value, list):
                    for child in value:
                        _walk(child)

            _walk(value_payload)
    return live


def _cmd_gc(args: argparse.Namespace) -> int:
    root = _runtime_root(args.runtime_root)
    artifact_dir = Path(args.artifact_dir).expanduser().resolve() if args.artifact_dir else _default_artifact_dir(root)
    object_dir = artifact_dir / "objects"
    meta_dir = _object_meta_dir(object_dir)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, int(args.older_than_hours)))
    deleted_objects: List[Dict[str, object]] = []
    kept_objects: List[Dict[str, object]] = []
    deleted_codes: List[Dict[str, object]] = []
    kept_codes: List[Dict[str, object]] = []

    if args.scope in ("codes", "all"):
        codes_dir = artifact_dir / "codes"
        if codes_dir.exists():
            for code_dir in sorted(path for path in codes_dir.iterdir() if path.is_dir()):
                meta_path = code_dir / "meta.json"
                meta = _load_json(meta_path)
                last_at_raw = str(meta.get("last_at", "") or "").strip()
                try:
                    last_at = datetime.fromisoformat(last_at_raw)
                    if last_at.tzinfo is None:
                        last_at = last_at.replace(tzinfo=timezone.utc)
                except Exception:
                    last_at = datetime.fromtimestamp(code_dir.stat().st_mtime, tz=timezone.utc)
                row = {
                    "code_sha": code_dir.name,
                    "path": str(code_dir),
                    "last_at": last_at.astimezone(timezone.utc).isoformat(),
                }
                if last_at >= cutoff:
                    row["reason"] = "recently_used"
                    kept_codes.append(row)
                    continue
                deleted_codes.append(row)
                if not args.dry_run:
                    shutil.rmtree(code_dir, ignore_errors=True)

    if args.scope in ("objects", "all") and object_dir.exists():
        live_object_ids = _collect_current_globals_object_ids(artifact_dir)
        for object_path in sorted(path for path in object_dir.iterdir() if path.is_file()):
            object_id = ""
            try:
                stem = object_path.name.split(".", 1)[0]
                object_id = normalize_object_id(f"sha256:{stem}")
            except Exception:
                continue

            meta_path = meta_dir / f"{object_path.name.split('.', 1)[0]}.json"
            meta = _load_json(meta_path)
            last_at_raw = str(meta.get("last_at", "") or "").strip()
            try:
                last_at = datetime.fromisoformat(last_at_raw)
                if last_at.tzinfo is None:
                    last_at = last_at.replace(tzinfo=timezone.utc)
            except Exception:
                last_at = datetime.fromtimestamp(object_path.stat().st_mtime, tz=timezone.utc)

            row = {
                "object_id": object_id,
                "path": str(object_path),
                "size_bytes": int(object_path.stat().st_size),
                "last_at": last_at.astimezone(timezone.utc).isoformat(),
            }
            if object_id in live_object_ids:
                row["reason"] = "referenced_by_current_globals"
                kept_objects.append(row)
                continue
            if last_at >= cutoff:
                row["reason"] = "recently_used"
                kept_objects.append(row)
                continue
            deleted_objects.append(row)
            if not args.dry_run:
                with contextlib.suppress(FileNotFoundError):
                    object_path.unlink()
                with contextlib.suppress(FileNotFoundError):
                    meta_path.unlink()

    payload = {
        "ok": True,
        "artifact_dir": str(artifact_dir),
        "dry_run": bool(args.dry_run),
        "scope": args.scope,
        "older_than_hours": int(args.older_than_hours),
        "deleted_objects": deleted_objects,
        "kept_objects": kept_objects,
        "deleted_codes": deleted_codes,
        "kept_codes": kept_codes,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def _add_env_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--env",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="extra environment variable passed to spawned service processes; may be repeated",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="PyCloud local service manager")
    parser.add_argument("--runtime-root", default="", help="base directory for logs and pid files (default: cwd or PYCLOUD_HOME)")
    parser.add_argument("--controlplane-port", type=int, default=50051)
    parser.add_argument("--node1-port", type=int, default=50061)
    parser.add_argument("--node1-http", type=int, default=18081)
    parser.add_argument("--node2-port", type=int, default=50062)
    parser.add_argument("--node2-http", type=int, default=18082)
    parser.add_argument("--node-worker-capacity", type=int, default=0, help="worker capacity per nodecontrol; default auto-calculated from CPU or PYCLOUD_NODE_WORKER_CAPACITY")

    subparsers = parser.add_subparsers(dest="command", required=True)
    start_parser = subparsers.add_parser("start", help="start controlplane and two nodecontrol processes")
    _add_env_argument(start_parser)
    start_infocenter = subparsers.add_parser("start-infocenter", help="start one local infocenter process")
    _add_env_argument(start_infocenter)
    start_infocenter.add_argument("--bind", default="0.0.0.0:50051")
    start_gateway = subparsers.add_parser("start-gateway", help="start one local gateway process")
    _add_env_argument(start_gateway)
    start_gateway.add_argument("--bind", default="0.0.0.0:50052")
    start_gateway.add_argument("--infocenter-addr", default="127.0.0.1:50051")
    start_gateway.add_argument("--gateway-refresh-interval-sec", type=float, default=3.0)
    start_gateway.add_argument("--gateway-failure-threshold", type=int, default=3)
    start_gateway.add_argument("--gateway-open-sec", type=float, default=5.0)
    start_controlplane = subparsers.add_parser("start-controlplane", help="start one local controlplane process")
    _add_env_argument(start_controlplane)
    start_controlplane.add_argument("--bind", default="0.0.0.0:50051")
    start_controlplane.add_argument("--gateway-refresh-interval-sec", type=float, default=3.0)
    start_controlplane.add_argument("--gateway-failure-threshold", type=int, default=3)
    start_controlplane.add_argument("--gateway-open-sec", type=float, default=5.0)
    start_node = subparsers.add_parser("start-node", help="start one local nodecontrol process")
    _add_env_argument(start_node)
    start_node.add_argument("--node-id", default="node-local-01", type=_normalize_managed_name)
    start_node.add_argument("--bind", default="0.0.0.0:50061")
    start_node.add_argument("--service-http-bind", default="127.0.0.1:18081")
    start_node.add_argument("--infocenter-addr", default=None, help="InfoCenter/ControlPlane target; omit to default to 127.0.0.1:<controlplane-port>, pass empty string to disable registration")
    start_node.add_argument("--advertise-addr", default="")
    start_node.add_argument("--worker-capacity", type=int, default=0)
    start_node.add_argument("--queue-capacity", type=int, default=NODE_QUEUE_CAPACITY)
    start_node.add_argument("--max-workers", type=int, default=NODE_MAX_WORKERS)
    start_node.add_argument("--service-default-workers", type=int, default=SERVICE_DEFAULT_WORKERS)
    start_node.add_argument("--service-heartbeat-timeout-sec", type=int, default=SERVICE_HEARTBEAT_TIMEOUT_SEC)
    start_node.add_argument("--node-tags", default="compute")
    start_node.add_argument("--node-version", default="v1")
    stop_parser = subparsers.add_parser("stop", help="stop local services started by pycloudctl")
    stop_parser.add_argument("--target", default="", help="InfoCenter/ControlPlane target for best-effort node cleanup before stop")
    stop_parser.add_argument("--scan-ports", action="store_true", help="after pid-based stop, scan configured ports and stop matching pycloud listener processes")
    stop_parser.add_argument("--ports", default="", help="comma-separated ports for doctor/scan-ports; default uses controlplane/node http+grpc ports")
    stop_node_parser = subparsers.add_parser("stop-node", help="stop one local nodecontrol process")
    stop_node_parser.add_argument("node_name", type=_normalize_managed_name, help="nodecontrol name to stop")
    stop_node_parser.add_argument("--target", default="", help="InfoCenter/ControlPlane target for best-effort node cleanup before stop")
    restart_parser = subparsers.add_parser("restart", help="restart local services")
    _add_env_argument(restart_parser)
    status_parser = subparsers.add_parser("status", help="show local service status")
    status_parser.add_argument("--target", default="", help="InfoCenter/ControlPlane target for node/service query (default: 127.0.0.1:<controlplane-port>)")
    doctor_parser = subparsers.add_parser("doctor", help="inspect runtime-root pid files and listener ports")
    doctor_parser.add_argument("--target", default="", help="InfoCenter/ControlPlane target for context display (default: 127.0.0.1:<controlplane-port>)")
    doctor_parser.add_argument("--ports", default="", help="comma-separated ports to inspect; default uses controlplane/node http+grpc ports")
    gc_parser = subparsers.add_parser("gc", help="garbage collect cached object files")
    gc_parser.add_argument("--artifact-dir", default="", help="artifact/code cache directory (default: <runtime-root>/code_cache)")
    gc_parser.add_argument("--scope", choices=["codes", "objects", "all"], default="all")
    gc_parser.add_argument("--older-than-hours", type=int, default=24 * 7)
    gc_parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "start":
        return _cmd_start(args)
    if args.command == "start-infocenter":
        return _cmd_start_infocenter(args)
    if args.command == "start-gateway":
        return _cmd_start_gateway(args)
    if args.command == "start-controlplane":
        return _cmd_start_controlplane(args)
    if args.command == "start-node":
        return _cmd_start_node(args)
    if args.command == "stop":
        return _cmd_stop(args)
    if args.command == "stop-node":
        return _cmd_stop_node(args)
    if args.command == "restart":
        return _cmd_restart(args)
    if args.command == "status":
        return _cmd_status(args)
    if args.command == "doctor":
        return _cmd_doctor(args)
    if args.command == "gc":
        return _cmd_gc(args)
    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
