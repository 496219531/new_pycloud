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
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid
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
from pycloud_parallel.controlplane.netutil import detect_local_ip, format_host_port as _net_format_host_port
from pycloud_parallel.controlplane.netutil import resolve_public_host, split_host_port as _net_split_host_port
from pycloud_parallel.data.ref import maybe_data_ref
from pycloud_parallel.data.ref import normalize_object_format, normalize_object_id, object_format_suffix, object_storage_path
from pycloud_parallel.controlplane.node.filesystem import (
    _code_index_link_path,
    _code_index_meta_path,
    _ensure_code_index_entry,
    _managed_globals_scope_dir,
)

_LOCALHOST = "127.0.0.1"


class _CtlArgumentParser(argparse.ArgumentParser):
    def parse_args(self, args=None, namespace=None):
        if args is not None:
            argv = list(args)
            local_commands = {
                "start",
                "start-infocenter",
                "start-gateway",
                "start-controlplane",
                "start-job-orchestrator",
                "start-node",
                "dev-start",
                "dev-restart",
                "restart",
            }
            command_index = next((idx for idx, token in enumerate(argv) if token in local_commands), -1)
            if command_index > 0:
                moved = False
                reordered: list[str] = []
                for idx, token in enumerate(argv):
                    if idx < command_index and token == "--local":
                        moved = True
                        continue
                    reordered.append(token)
                if moved:
                    insert_at = reordered.index(argv[command_index]) + 1
                    reordered.insert(insert_at, "--local")
                    args = reordered
        parsed = super().parse_args(args=args, namespace=namespace)
        parsed.local = bool(getattr(parsed, "local", False) or getattr(parsed, "_global_local", False))
        if hasattr(parsed, "_global_local"):
            delattr(parsed, "_global_local")
        return parsed


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
        try:
            return int(env_value)
        except Exception:
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
    return _net_split_host_port(bind)


def _format_host_port(host: str, port: int) -> str:
    return _net_format_host_port(host, port)


def _resolve_bind_value(
    bind: str,
    *,
    host: str = "",
    port: int = 0,
    label: str = "bind",
    remote_hint: str = "",
    prefer_local: bool = False,
) -> str:
    base_host, base_port = _split_host_port(bind)
    host_override = str(host or "").strip()
    if host_override:
        resolved_host = host_override
    else:
        resolved_host = base_host
        if prefer_local and str(base_host or "").strip() in {"", "0.0.0.0", "::", "[::]"}:
            resolved_host = _LOCALHOST
    resolved_host = resolve_public_host(resolved_host, remote_hint=remote_hint)
    resolved_port = int(port or 0) or int(base_port)
    if resolved_port <= 0 or resolved_port > 65535:
        raise ValueError(f"{label} port must be between 1 and 65535, got {resolved_port}")
    return _format_host_port(resolved_host, resolved_port)


def _default_service_http_bind_for_node_bind(bind: str) -> str:
    host, control_port = _split_host_port(bind)
    http_port = 18081
    if int(control_port) >= 50061:
        http_port = int(control_port) - 31980
    return _format_host_port(host, http_port)


def _probe_host(host: str) -> str:
    text = str(host or "").strip()
    if text in {"", "0.0.0.0", "::", "[::]"}:
        return "127.0.0.1"
    return text


def _default_bind_host(*, remote_hint: str = "") -> str:
    return detect_local_ip(remote_hint=remote_hint)


def _default_host_for_args(args: argparse.Namespace, *, remote_hint: str = "") -> str:
    if bool(getattr(args, "local", False)):
        return _LOCALHOST
    return _default_bind_host(remote_hint=remote_hint)


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
    if bind_host in {"", "0.0.0.0"}:
        family = socket.AF_INET
        bind_host = "0.0.0.0"
    elif bind_host in {"::", "[::]"}:
        family = socket.AF_INET6
        bind_host = "::"
    else:
        family = socket.AF_INET6 if ":" in bind_host else socket.AF_INET
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
    if name == "job-orchestrator":
        return (1, name)
    if name == "gateway":
        return (2, name)
    if name == "controlplane":
        return (3, name)
    if name == "infocenter":
        return (4, name)
    return (4, name)


def _managed_process_names(root: Path) -> List[str]:
    names = {"controlplane", "job-orchestrator"}
    names.update(_pid_names(root))
    return sorted(names, key=_process_sort_key)


def _running_managed_processes(root: Path) -> List[Tuple[str, int]]:
    running: List[Tuple[str, int]] = []
    for name in _managed_process_names(root):
        pid = _read_pid(_pid_file(root, name))
        if pid > 0 and _is_pid_running(pid):
            running.append((name, pid))
    return running


def _stop_all_managed_processes(root: Path) -> None:
    for name in _managed_process_names(root):
        _stop_named_process(root, name)


def _core_process_names(root: Path) -> List[str]:
    del root
    return ["job-orchestrator", "controlplane"]


def _stop_core_processes(root: Path) -> None:
    for name in _core_process_names(root):
        _stop_named_process(root, name)


def _default_named_ports(args: argparse.Namespace) -> Dict[str, int]:
    node_control_port = int(getattr(args, "node_control_port", 50061) or 50061)
    node_service_http_port = int(getattr(args, "node_service_http_port", 18081) or 18081)
    return {
        "controlplane": int(args.controlplane_port),
        "job-orchestrator": int(args.job_orchestrator_port),
        "node-1-control": node_control_port,
        "node-1-http": node_service_http_port,
        "node-2-control": node_control_port + 1,
        "node-2-http": node_service_http_port + 1,
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


def _windows_shell_candidates() -> List[str]:
    candidates: List[str] = []
    seen: set[str] = set()

    def _add(value: str) -> None:
        text = str(value or "").strip()
        if not text:
            return
        normalized = text.lower()
        if normalized in seen:
            return
        seen.add(normalized)
        candidates.append(text)

    system_root = str(os.environ.get("SystemRoot", "") or os.environ.get("WINDIR", "")).strip()
    if system_root:
        _add(str(Path(system_root) / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"))
    for key in ("ProgramFiles", "ProgramW6432", "ProgramFiles(x86)"):
        base = str(os.environ.get(key, "") or "").strip()
        if base:
            _add(str(Path(base) / "PowerShell" / "7" / "pwsh.exe"))
    for name in ("powershell.exe", "powershell", "pwsh.exe", "pwsh"):
        resolved = shutil.which(name)
        if resolved:
            _add(resolved)
        _add(name)
    return candidates


def _run_windows_shell(command: str) -> subprocess.CompletedProcess[str] | None:
    shell_command = str(command or "").strip()
    if not shell_command:
        return None
    for executable in _windows_shell_candidates():
        try:
            return subprocess.run(
                [executable, "-NoProfile", "-Command", shell_command],
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError:
            continue
    return None


def _command_for_pid(pid: int) -> str:
    if pid <= 0:
        return ""
    if os.name == "nt":
        ps_result = _run_windows_shell(
            f"(Get-CimInstance Win32_Process -Filter \"ProcessId = {int(pid)}\").CommandLine"
        )
        text = str(getattr(ps_result, "stdout", "") or "").strip()
        if text:
            return text
        try:
            tasklist_result = subprocess.run(
                ["tasklist", "/FO", "CSV", "/NH", "/FI", f"PID eq {int(pid)}"],
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError:
            return ""
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
        "pycloud_parallel",
        "pycloud controlplane",
        "pycloudctl",
        "start_services.sh",
        "start_services.bat",
    )
    return any(needle in text for needle in needles)


def _looks_like_stoppable_pycloud_server_process(command: str) -> bool:
    text = str(command or "").strip().lower()
    if not text:
        return False
    if "--role" not in text:
        return False
    return (
        "pycloud_parallel.controlplane.server" in text
        or "pycloud-control" in text
    )


def _role_from_command(command: str) -> str:
    match = re.search(r"--role\s+([A-Za-z0-9_-]+)", str(command or ""))
    role = str(match.group(1) if match else "").strip().lower()
    if role.startswith("info"):
        return "infocenter"
    if role.startswith("gate"):
        return "gateway"
    if role.startswith("job"):
        return "joborchestrator"
    if role.startswith("node"):
        return "nodecontrol"
    if role.startswith("cont"):
        return "controlplane"
    return role


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


def _inspect_machine_processes() -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    current_pid = int(os.getpid())
    if os.name == "nt":
        result = _run_windows_shell(
            "Get-CimInstance Win32_Process | Select-Object ProcessId,CommandLine | ConvertTo-Json -Compress"
        )
        payload = str(getattr(result, "stdout", "") or "").strip()
        if not payload:
            return rows
        try:
            items = json.loads(payload)
        except Exception:
            return rows
        if isinstance(items, dict):
            items = [items]
        for item in items if isinstance(items, list) else []:
            if not isinstance(item, dict):
                continue
            try:
                pid = int(item.get("ProcessId", 0) or 0)
            except Exception:
                continue
            command = str(item.get("CommandLine", "") or "").strip()
            if pid <= 0 or pid == current_pid or not command:
                continue
            rows.append(
                {
                    "pid": pid,
                    "command": command,
                    "matches_pycloud": _looks_like_pycloud_process(command),
                    "matches_stoppable_server": _looks_like_stoppable_pycloud_server_process(command),
                    "role": _role_from_command(command),
                    "node_name": _node_name_from_command(command),
                }
            )
        return rows

    result = subprocess.run(
        ["ps", "-ax", "-o", "pid=", "-o", "command="],
        capture_output=True,
        text=True,
        check=False,
    )
    for raw in (result.stdout or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split(None, 1)
        if not parts:
            continue
        try:
            pid = int(parts[0])
        except Exception:
            continue
        command = parts[1].strip() if len(parts) > 1 else ""
        if pid <= 0 or pid == current_pid or not command:
            continue
        rows.append(
            {
                "pid": pid,
                "command": command,
                "matches_pycloud": _looks_like_pycloud_process(command),
                "matches_stoppable_server": _looks_like_stoppable_pycloud_server_process(command),
                "role": _role_from_command(command),
                "node_name": _node_name_from_command(command),
            }
        )
    return rows


def _managed_name_from_process_row(row: Dict[str, object]) -> str:
    role = str(row.get("role", "") or "").strip()
    if role == "nodecontrol":
        return str(row.get("node_name", "") or "").strip()
    if role == "joborchestrator":
        return "job-orchestrator"
    if role in {"infocenter", "gateway", "controlplane"}:
        return role
    return ""


def _kill_machine_pycloud_processes(*, root: Path, target: str) -> List[Dict[str, object]]:
    killed: List[Dict[str, object]] = []
    seen_pids: set[int] = set()
    rows = []
    for row in _inspect_machine_processes():
        pid = int(row.get("pid", 0) or 0)
        if pid <= 0 or pid in seen_pids:
            continue
        seen_pids.add(pid)
        if not bool(row.get("matches_stoppable_server", False)):
            continue
        role = str(row.get("role", "") or "").strip()
        if role not in {"infocenter", "controlplane", "gateway", "joborchestrator", "nodecontrol"}:
            continue
        rows.append(dict(row))

    rows.sort(key=lambda item: _process_sort_key(_managed_name_from_process_row(item) or str(item.get("role", "") or "")))

    for row in rows:
        pid = int(row.get("pid", 0) or 0)
        if pid <= 0:
            continue
        node_name = str(row.get("node_name", "") or "").strip()
        role = str(row.get("role", "") or "").strip()
        managed_name = _managed_name_from_process_row(row)
        if role == "nodecontrol" and node_name:
            _best_effort_mark_node_lost(target, node_name)
        _log("INFO", f"Stopping discovered process PID {pid} ({managed_name or role})...")
        _terminate_pid(pid, force=False)
        deadline = time.time() + 3.0
        while time.time() < deadline:
            if not _is_pid_running(pid):
                break
            time.sleep(0.2)
        if _is_pid_running(pid):
            _terminate_pid(pid, force=True)
        if managed_name:
            _remove_pid(_pid_file(root, managed_name))
        killed.append(dict(row))
    return killed


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
        if role and role not in {"infocenter", "controlplane", "gateway", "joborchestrator", "nodecontrol"}:
            continue
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
            remaining = max(0.05, deadline - time.time())
            with urlopen(url, timeout=min(1.0, remaining)) as resp:
                data = json.loads(resp.read().decode("utf-8") or "{}")
            if isinstance(data, dict) and data.get("ok") is True:
                return True
        except Exception:
            pass
        time.sleep(min(0.2, max(0.01, deadline - time.time())))
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


def _wait_service_registered(infocenter_target: str, service_name: str, timeout_sec: float) -> bool:
    target = str(infocenter_target or "").strip()
    if not target.startswith(("http://", "https://")):
        target = f"http://{target}"
    url = (
        f"{target.rstrip('/')}/services/routes"
        f"?service_name={quote(str(service_name or '').strip(), safe='')}&healthy_only=true&limit=100"
    )
    deadline = time.time() + max(0.1, float(timeout_sec))
    while time.time() < deadline:
        try:
            with urlopen(url, timeout=1.0) as resp:
                data = json.loads(resp.read().decode("utf-8") or "{}")
            routes = data.get("routes") or []
            if any(isinstance(item, dict) for item in routes):
                return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def _is_local_target(target: str) -> bool:
    return str(target or "").strip().lower() == "local"


def _wait_local_service_registered(service_name: str, timeout_sec: float) -> bool:
    from pycloud_parallel.controlplane.local_ipc import inspect_local_services

    deadline = time.time() + max(0.1, float(timeout_sec))
    normalized = str(service_name or "").strip()
    while time.time() < deadline:
        try:
            rows = inspect_local_services(timeout_sec=0.2)
            for row in rows:
                if str(row.get("service_name", "") or "").strip() == normalized and bool(row.get("alive", False)):
                    return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def _stop_existing_local_ipc_service(service_name: str, *, timeout_sec: float = 3.0) -> None:
    from pycloud_parallel.controlplane.local_ipc import cleanup_stale_local_services, stop_local_service

    with contextlib.suppress(FileNotFoundError):
        stop_local_service(str(service_name or "").strip(), timeout_sec=timeout_sec, force=True, cleanup=True)
    with contextlib.suppress(Exception):
        cleanup_stale_local_services(timeout_sec=0.2)


def _find_local_service_pid(service_name: str) -> int:
    from pycloud_parallel.controlplane.local_ipc import inspect_local_services

    normalized = str(service_name or "").strip()
    try:
        rows = inspect_local_services(timeout_sec=0.2)
    except Exception:
        return 0
    for row in rows:
        if str(row.get("service_name", "") or "").strip() != normalized:
            continue
        if not bool(row.get("alive", False)):
            continue
        try:
            return int(row.get("pid", 0) or 0)
        except Exception:
            return 0
    return 0


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


def _spawn_server_debug_macos_terminal(
    root: Path,
    log_path: Path,
    args: Iterable[str],
    *,
    env: Dict[str, str],
) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = _logs_dir(root) / ".debug-launch"
    temp_dir.mkdir(parents=True, exist_ok=True)
    token = uuid.uuid4().hex
    pid_path = temp_dir / f"{log_path.stem}-{token}.pid"
    script_path = temp_dir / f"{log_path.stem}-{token}.sh"
    quoted_command = " ".join(shlex.quote(part) for part in _server_command(*args))
    lines = [
        "#!/bin/bash",
        "set -e",
        f"cd {shlex.quote(str(root))}",
        f"echo $$ > {shlex.quote(str(pid_path))}",
        "rm -- \"$0\"",
    ]
    for key, value in sorted(env.items()):
        lines.append(f"export {key}={shlex.quote(str(value))}")
    lines.append(f"exec {quoted_command}")
    script_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    script_path.chmod(0o755)

    command_text = f"bash {shlex.quote(str(script_path))}"
    subprocess.run(
        [
            "osascript",
            "-e",
            'tell application "Terminal" to activate',
            "-e",
            f'tell application "Terminal" to do script {json.dumps(command_text)}',
        ],
        check=True,
        cwd=str(root),
        env=env,
    )

    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            raw = pid_path.read_text(encoding="utf-8").strip()
            pid_path.unlink(missing_ok=True)
            return int(raw)
        except FileNotFoundError:
            time.sleep(0.05)
        except ValueError:
            break
    return 0


def _spawn_server(
    root: Path,
    log_path: Path,
    args: Iterable[str],
    *,
    extra_env: Dict[str, str] | None = None,
    debug: bool = False,
) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    if extra_env:
        env.update({str(k): str(v) for k, v in extra_env.items()})
    if os.name == "nt":
        creationflags = int(getattr(subprocess, "CREATE_NEW_CONSOLE", 0) or 0)
        proc = subprocess.Popen(
            _server_command(*args),
            stdout=None if debug else None,
            stderr=None if debug else None,
            cwd=str(root),
            env=env,
            close_fds=False,
            creationflags=creationflags,
        )
    else:
        if debug and sys.platform == "darwin":
            return _spawn_server_debug_macos_terminal(
                root,
                log_path,
                args,
                env=env,
            )
        if debug:
            proc = subprocess.Popen(
                _server_command(*args),
                cwd=str(root),
                env=env,
                close_fds=True,
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


def _start_controlplane(
    root: Path,
    port: int,
    *,
    bind_host: str = "0.0.0.0",
    remote_hint: str = "",
    extra_env: Dict[str, str] | None = None,
    debug: bool = False,
) -> None:
    effective_bind_host = resolve_public_host(str(bind_host or "").strip() or _default_bind_host(remote_hint=remote_hint), remote_hint=remote_hint)
    bind = _format_host_port(effective_bind_host, int(port))
    _assert_bind_available(bind)
    _log("INFO", f"Starting ControlPlane on {bind}...")
    pid = _spawn_server(
        root,
        _logs_dir(root) / "controlplane.log",
        ["--role", "controlplane", "--bind", bind, "--log-level", "DEBUG" if debug else "INFO"],
        extra_env=extra_env,
        debug=debug,
    )
    ready_host = _probe_host(effective_bind_host)
    if not _wait_ready_with_pid(pid, 15.0, lambda: _wait_http_json_ok(ready_host, int(port), 0.5, path="/nodes?healthy_only=false&limit=1")):
        _remove_pid(_pid_file(root, "controlplane"))
        raise RuntimeError("ControlPlane failed to become ready")
    _write_pid(_pid_file(root, "controlplane"), pid)
    _log("OK", f"ControlPlane started (PID: {pid}, Bind: {bind})")


def _start_infocenter(root: Path, *, bind: str, extra_env: Dict[str, str] | None = None, debug: bool = False) -> None:
    host, port = _split_host_port(bind)
    _assert_bind_available(bind)
    _log("INFO", f"Starting InfoCenter on {bind}...")
    pid = _spawn_server(
        root,
        _logs_dir(root) / "infocenter.log",
        ["--role", "infocenter", "--bind", bind, "--log-level", "DEBUG" if debug else "INFO"],
        extra_env=extra_env,
        debug=debug,
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
    debug: bool = False,
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
            "DEBUG" if debug else "INFO",
        ],
        extra_env=extra_env,
        debug=debug,
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
    debug: bool = False,
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
            "--target",
            infocenter_addr,
            "--gateway-refresh-interval-sec",
            f"{float(gateway_refresh_interval_sec):.3f}",
            "--gateway-failure-threshold",
            str(int(gateway_failure_threshold)),
            "--gateway-open-sec",
            f"{float(gateway_open_sec):.3f}",
            "--log-level",
            "DEBUG" if debug else "INFO",
        ],
        extra_env=extra_env,
        debug=debug,
    )
    if not _wait_ready_with_pid(pid, 15.0, lambda: _wait_http_ready(bind, 0.2, path="/svc/__pycloudctl_probe__/status")):
        _remove_pid(_pid_file(root, "gateway"))
        raise RuntimeError("Gateway failed to become ready")
    _write_pid(_pid_file(root, "gateway"), pid)
    _log("OK", f"Gateway started (PID: {pid}, Bind: {bind}, InfoCenter: {infocenter_addr})")


def _start_job_orchestrator(
    root: Path,
    *,
    bind: str,
    infocenter_addr: str,
    node_id: str = "job-orchestrator-01",
    service_name: str = "job-orchestrator",
    queue_capacity: int = NODE_QUEUE_CAPACITY,
    node_tags: str = "job",
    node_version: str = "v1",
    extra_env: Dict[str, str] | None = None,
    debug: bool = False,
    force: bool = False,
) -> None:
    _assert_bind_available(bind)
    local_target = _is_local_target(infocenter_addr)
    if local_target:
        existing_pid = _find_local_service_pid(service_name)
        if force:
            if existing_pid:
                _log("WARN", f"Replacing existing local IPC service {service_name} (PID: {existing_pid}) because --force was set")
            _stop_existing_local_ipc_service(service_name)
        else:
            if existing_pid:
                raise RuntimeError(
                    f"local service_name already exists: {service_name}\n"
                    f"pid={existing_pid}\n"
                    f"stop with: pycloudctl stop-local-service {service_name}\n"
                    f"or re-run with: pycloudctl start-job-orchestrator --target local --force"
                )
    _log("INFO", f"Starting JobOrchestrator on {bind} (InfoCenter: {infocenter_addr}, service: {service_name})...")
    pid = _spawn_server(
        root,
        _logs_dir(root) / "job-orchestrator.log",
        [
            "--role",
            "joborchestrator",
            "--bind",
            bind,
            "--target",
            infocenter_addr,
            "--node-id",
            node_id,
            "--service-name",
            service_name,
            "--queue-capacity",
            str(int(queue_capacity)),
            "--node-tags",
            node_tags,
            "--node-version",
            node_version,
            "--log-level",
            "DEBUG" if debug else "INFO",
            *(["--force"] if force else []),
        ],
        extra_env=extra_env,
        debug=debug,
    )
    if local_target:
        is_ready = _wait_ready_with_pid(pid, 15.0, lambda: _wait_local_service_registered(service_name, 0.2))
        error_message = "JobOrchestrator failed to start local IPC service"
    else:
        is_ready = _wait_ready_with_pid(pid, 15.0, lambda: _wait_service_registered(infocenter_addr, service_name, 0.2))
        error_message = "JobOrchestrator failed to register to InfoCenter"
    if not is_ready:
        _remove_pid(_pid_file(root, "job-orchestrator"))
        raise RuntimeError(error_message)
    _write_pid(_pid_file(root, "job-orchestrator"), pid)
    _log("OK", f"JobOrchestrator started (PID: {pid}, Bind: {bind}, InfoCenter: {infocenter_addr})")


def _start_node(
    root: Path,
    name: str,
    port: int,
    http_port: int,
    infocenter_target: str,
    worker_capacity: int,
    *,
    bind_host: str = "",
    service_http_host: str = "",
    advertise_host: str = "",
    queue_capacity: int = NODE_QUEUE_CAPACITY,
    max_workers: int = NODE_MAX_WORKERS,
    service_default_workers: int = SERVICE_DEFAULT_WORKERS,
    service_heartbeat_timeout_sec: int = SERVICE_HEARTBEAT_TIMEOUT_SEC,
    extra_env: Dict[str, str] | None = None,
    debug: bool = False,
) -> None:
    effective_bind_host = resolve_public_host(
        str(bind_host or "").strip() or _default_bind_host(remote_hint=infocenter_target),
        remote_hint=infocenter_target,
    )
    effective_service_http_host = resolve_public_host(
        str(service_http_host or "").strip() or _default_bind_host(remote_hint=infocenter_target),
        remote_hint=infocenter_target,
    )
    effective_advertise_host = str(advertise_host or "").strip() or resolve_public_host(effective_bind_host, remote_hint=infocenter_target)
    control_bind = _format_host_port(effective_bind_host, int(port))
    service_http_bind = _format_host_port(effective_service_http_host, int(http_port))
    advertise_addr = _format_host_port(effective_advertise_host, int(port))
    _assert_bind_available(control_bind)
    _assert_bind_available(service_http_bind)
    _log("INFO", f"Starting {name} on {control_bind} (HTTP: {service_http_bind}, workers: {worker_capacity})...")
    pid = _spawn_server(
        root,
        _logs_dir(root) / f"{name}.log",
        [
            "--role",
            "nodecontrol",
            "--bind",
            control_bind,
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
            service_http_bind,
            "--target",
            infocenter_target,
            "--advertise-addr",
            advertise_addr,
            "--node-tags",
            "compute",
            "--log-level",
            "DEBUG" if debug else "INFO",
        ],
        extra_env=extra_env,
        debug=debug,
    )
    if not _wait_ready_with_pid(pid, 15.0, lambda: _wait_node_registered(infocenter_target, name, 0.2)):
        _remove_pid(_pid_file(root, name))
        raise RuntimeError(f"{name} failed to register to InfoCenter")
    _write_pid(_pid_file(root, name), pid)
    _log("OK", f"{name} started (PID: {pid}, Bind: {control_bind}, HTTP: {service_http_bind}, workers: {worker_capacity})")


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
    debug: bool = False,
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
        "DEBUG" if debug else "INFO",
    ]
    if infocenter_addr:
        args.extend(["--target", infocenter_addr])
    if advertise_addr:
        args.extend(["--advertise-addr", advertise_addr])
    pid = _spawn_server(root, _logs_dir(root) / f"{node_id}.log", args, extra_env=extra_env, debug=debug)
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
        from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient

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
    controlplane_host = resolve_public_host(str(getattr(args, "controlplane_host", "") or "") or _default_host_for_args(args))
    job_orchestrator_host = resolve_public_host(str(getattr(args, "job_orchestrator_host", "") or "") or _default_host_for_args(args))

    _log("INFO", "Stopping existing core services...")
    _stop_core_processes(root)
    time.sleep(1.0)

    infocenter_target = _format_host_port(controlplane_host, int(args.controlplane_port))
    debug = bool(getattr(args, "debug", False))
    controlplane_kwargs = dict(
        bind_host=controlplane_host,
        remote_hint=infocenter_target,
        extra_env=extra_env,
    )
    job_orchestrator_kwargs = dict(
        infocenter_addr=infocenter_target,
        extra_env=extra_env,
    )
    if debug:
        controlplane_kwargs["debug"] = True
        job_orchestrator_kwargs["debug"] = True
    _start_controlplane(root, args.controlplane_port, **controlplane_kwargs)
    _start_job_orchestrator(
        root,
        bind=_format_host_port(job_orchestrator_host, int(args.job_orchestrator_port)),
        **job_orchestrator_kwargs,
    )

    print("============================================")
    print("  Core Services Started!")
    print("============================================")
    print()
    print(f"  ControlPlane: {_format_host_port(controlplane_host, int(args.controlplane_port))}")
    print(f"  JobQueue:     {_format_host_port(job_orchestrator_host, int(args.job_orchestrator_port))}")
    print(f"  Logs:        {_logs_dir(root)}")
    print(f"  PIDs:        {_pids_dir(root)}")
    return 0


def _cmd_dev_start(args: argparse.Namespace) -> int:
    root = _runtime_root(args.runtime_root)
    _ensure_runtime_dirs(root)
    extra_env = _parse_env_overrides(getattr(args, "env", []) or [])
    controlplane_host = resolve_public_host(str(getattr(args, "controlplane_host", "") or "") or _default_host_for_args(args))
    job_orchestrator_host = resolve_public_host(str(getattr(args, "job_orchestrator_host", "") or "") or _default_host_for_args(args))

    _log("INFO", "Stopping existing managed dev services...")
    _stop_all_managed_processes(root)
    time.sleep(1.0)

    infocenter_target = _format_host_port(controlplane_host, int(args.controlplane_port))
    debug = bool(getattr(args, "debug", False))
    controlplane_kwargs = dict(
        bind_host=controlplane_host,
        remote_hint=infocenter_target,
        extra_env=extra_env,
    )
    job_orchestrator_kwargs = dict(
        infocenter_addr=infocenter_target,
        extra_env=extra_env,
    )
    if debug:
        controlplane_kwargs["debug"] = True
        job_orchestrator_kwargs["debug"] = True
    _start_controlplane(root, args.controlplane_port, **controlplane_kwargs)
    _start_job_orchestrator(
        root,
        bind=_format_host_port(job_orchestrator_host, int(args.job_orchestrator_port)),
        **job_orchestrator_kwargs,
    )
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
    node_kwargs = dict(
        queue_capacity=queue_capacity,
        max_workers=max_workers,
        service_default_workers=service_default_workers,
        service_heartbeat_timeout_sec=service_heartbeat_timeout_sec,
        extra_env=extra_env,
    )
    if debug:
        node_kwargs["debug"] = True

    node_count = max(0, int(getattr(args, "nodes", 2) or 0))
    node_base_port = int(getattr(args, "node_control_port", 50061) or 50061)
    service_http_base_port = int(getattr(args, "node_service_http_port", 18081) or 18081)
    node_host_seed = str(getattr(args, "node_host", "") or "") or _default_host_for_args(args)
    service_http_host_seed = str(getattr(args, "node_service_http_host", "") or "") or _default_host_for_args(args)
    node_host = resolve_public_host(node_host_seed)
    service_http_host = resolve_public_host(service_http_host_seed)
    for index in range(node_count):
        node_name = f"node-{index + 1}"
        _start_node(
            root,
            node_name,
            node_base_port + index,
            service_http_base_port + index,
            infocenter_target,
            worker_capacity,
            bind_host=node_host,
            service_http_host=service_http_host,
            advertise_host=node_host,
            **node_kwargs,
        )

    print("============================================")
    print("  Dev Services Started!")
    print("============================================")
    print()
    print(f"  ControlPlane: {_format_host_port(controlplane_host, int(args.controlplane_port))}")
    print(f"  JobQueue:     {_format_host_port(job_orchestrator_host, int(args.job_orchestrator_port))}")
    for index in range(node_count):
        print(
            f"  Node-{index + 1}:      {_format_host_port(node_host, node_base_port + index)} "
            f"(HTTP: {_format_host_port(service_http_host, service_http_base_port + index)})"
        )
    if node_count:
        print(f"  Worker cap:  {worker_capacity} per node")
    print(f"  Logs:        {_logs_dir(root)}")
    print(f"  PIDs:        {_pids_dir(root)}")
    return 0


def _cmd_stop(args: argparse.Namespace) -> int:
    root = _runtime_root(args.runtime_root)
    target = str(getattr(args, "target", "") or f"127.0.0.1:{int(args.controlplane_port)}")
    del target
    _stop_core_processes(root)
    if bool(getattr(args, "scan_ports", False)):
        scanned_ports = _collect_scan_ports(args)
        scanned = _kill_scanned_port_processes(target=f"127.0.0.1:{int(args.controlplane_port)}", ports=scanned_ports)
        if scanned:
            _log("OK", f"Stopped {len(scanned)} additional scanned listener process(es)")
    _log("OK", "Core services stopped")
    return 0


def _stop_local_ipc_services(*, timeout_sec: float = 3.0, force: bool = True) -> Tuple[int, int]:
    from pycloud_parallel.controlplane.local_ipc import cleanup_stale_local_services, iter_local_service_metadata, stop_local_service

    stopped = 0
    failed = 0
    for meta in iter_local_service_metadata():
        service_name = str(meta.get("service_name", "") or "").strip()
        if not service_name:
            continue
        try:
            result = stop_local_service(service_name, timeout_sec=timeout_sec, force=force, cleanup=True)
            if bool(result.get("stopped", False)):
                stopped += 1
            else:
                failed += 1
        except Exception:
            failed += 1
    cleanup_stale_local_services(timeout_sec=0.2)
    return stopped, failed


def _cmd_stopall(args: argparse.Namespace) -> int:
    root = _runtime_root(args.runtime_root)
    target = str(getattr(args, "target", "") or f"127.0.0.1:{int(args.controlplane_port)}")
    for name in _managed_process_names(root):
        if name not in {"controlplane", "gateway", "infocenter", "job-orchestrator"}:
            _best_effort_mark_node_lost(target, name)
    _stop_all_managed_processes(root)
    killed = _kill_machine_pycloud_processes(root=root, target=target)
    if killed:
        _log("OK", f"Stopped {len(killed)} additional machine-wide pycloud process(es)")
    stopped_local, failed_local = _stop_local_ipc_services(
        timeout_sec=float(getattr(args, "timeout_sec", 3.0) or 3.0),
        force=True,
    )
    if stopped_local:
        _log("OK", f"Stopped {stopped_local} local IPC service(s)")
    if failed_local:
        _log("ERROR", f"Failed to stop {failed_local} local IPC service(s)")
    if bool(getattr(args, "scan_ports", False)):
        scanned_ports = _collect_scan_ports(args)
        scanned = _kill_scanned_port_processes(target=target, ports=scanned_ports)
        if scanned:
            _log("OK", f"Stopped {len(scanned)} additional scanned listener process(es)")
    _log("OK", "All pycloud services stopped")
    return 1 if failed_local else 0


def _cmd_dev_stop(args: argparse.Namespace) -> int:
    root = _runtime_root(args.runtime_root)
    target = str(getattr(args, "target", "") or f"127.0.0.1:{int(args.controlplane_port)}")
    for name in _managed_process_names(root):
        if name not in {"controlplane", "gateway", "infocenter", "job-orchestrator"}:
            _best_effort_mark_node_lost(target, name)
    _stop_all_managed_processes(root)
    _log("OK", "Dev services stopped")
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
    bind = _resolve_bind_value(
        str(args.bind),
        host="",
        port=0,
        label="infocenter bind",
        prefer_local=bool(getattr(args, "local", False)),
    )
    _start_infocenter(
        root,
        bind=bind,
        extra_env=_parse_env_overrides(getattr(args, "env", []) or []),
        **({"debug": True} if bool(getattr(args, "debug", False)) else {}),
    )
    return 0


def _cmd_start_gateway(args: argparse.Namespace) -> int:
    root = _runtime_root(args.runtime_root)
    _ensure_runtime_dirs(root)
    _stop_named_process(root, "gateway")
    extra_env = _parse_env_overrides(getattr(args, "env", []) or [])
    infocenter_addr = str(getattr(args, "infocenter_addr", "") or "").strip()
    if not infocenter_addr:
        raise RuntimeError("start-gateway requires --target; pycloudctl no longer defaults to a local InfoCenter target")
    bind = _resolve_bind_value(
        str(args.bind),
        host="",
        port=0,
        label="gateway bind",
        remote_hint=infocenter_addr,
        prefer_local=bool(getattr(args, "local", False)),
    )
    _start_gateway(
        root,
        bind=bind,
        infocenter_addr=infocenter_addr,
        gateway_refresh_interval_sec=float(args.gateway_refresh_interval_sec),
        gateway_failure_threshold=int(args.gateway_failure_threshold),
        gateway_open_sec=float(args.gateway_open_sec),
        extra_env=extra_env,
        **({"debug": True} if bool(getattr(args, "debug", False)) else {}),
    )
    return 0


def _cmd_start_controlplane(args: argparse.Namespace) -> int:
    root = _runtime_root(args.runtime_root)
    _ensure_runtime_dirs(root)
    _stop_named_process(root, "controlplane")
    extra_env = _parse_env_overrides(getattr(args, "env", []) or [])
    bind = _resolve_bind_value(
        str(args.bind),
        host="",
        port=0,
        label="controlplane bind",
        prefer_local=bool(getattr(args, "local", False)),
    )
    _start_standalone_controlplane(
        root,
        bind=bind,
        gateway_refresh_interval_sec=float(args.gateway_refresh_interval_sec),
        gateway_failure_threshold=int(args.gateway_failure_threshold),
        gateway_open_sec=float(args.gateway_open_sec),
        extra_env=extra_env,
        **({"debug": True} if bool(getattr(args, "debug", False)) else {}),
    )
    return 0


def _cmd_start_job_orchestrator(args: argparse.Namespace) -> int:
    root = _runtime_root(args.runtime_root)
    _ensure_runtime_dirs(root)
    _stop_named_process(root, "job-orchestrator")
    extra_env = _parse_env_overrides(getattr(args, "env", []) or [])
    infocenter_addr = str(getattr(args, "infocenter_addr", "") or "").strip()
    if not infocenter_addr:
        raise RuntimeError("start-job-orchestrator requires --target")
    bind = _resolve_bind_value(
        str(args.bind),
        host="",
        port=0,
        label="job orchestrator bind",
        remote_hint=infocenter_addr,
        prefer_local=bool(getattr(args, "local", False)),
    )
    _start_job_orchestrator(
        root,
        bind=bind,
        infocenter_addr=infocenter_addr,
        node_id=str(getattr(args, "node_id", "") or "job-orchestrator-01"),
        service_name=str(getattr(args, "service_name", "") or "job-orchestrator"),
        queue_capacity=int(getattr(args, "queue_capacity", NODE_QUEUE_CAPACITY)),
        node_tags=str(getattr(args, "node_tags", "") or "job"),
        node_version=str(getattr(args, "node_version", "") or "v1"),
        extra_env=extra_env,
        force=bool(getattr(args, "force", False)),
        **({"debug": True} if bool(getattr(args, "debug", False)) else {}),
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
    if infocenter_arg is None:
        raise RuntimeError(
            'start-node requires an explicit --target; pass --target "" to start a standalone node without registration'
        )
    infocenter_addr = str(infocenter_arg or "").strip()
    bind = _resolve_bind_value(
        str(args.bind),
        host="",
        port=0,
        label="node bind",
        remote_hint=infocenter_addr,
        prefer_local=bool(getattr(args, "local", False)),
    )
    service_http_seed = str(getattr(args, "service_http_bind", "") or "").strip()
    if not service_http_seed:
        service_http_seed = _default_service_http_bind_for_node_bind(bind)
    service_http_bind = _resolve_bind_value(
        service_http_seed,
        host="",
        port=0,
        label="service http bind",
        remote_hint=infocenter_addr,
        prefer_local=bool(getattr(args, "local", False)),
    )
    advertise_addr = str(args.advertise_addr or "").strip()
    if advertise_addr:
        advertise_addr = _resolve_bind_value(
            advertise_addr,
            host="",
            port=0,
            label="advertise addr",
            prefer_local=bool(getattr(args, "local", False)),
        )
    if infocenter_addr and not advertise_addr:
        host, port = _split_host_port(bind)
        advertise_addr = _format_host_port(resolve_public_host(host, remote_hint=infocenter_addr), int(port))
    _start_standalone_node(
        root,
        node_id=node_id,
        bind=bind,
        service_http_bind=service_http_bind,
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
        **({"debug": True} if bool(getattr(args, "debug", False)) else {}),
    )
    return 0


def _cmd_restart(args: argparse.Namespace) -> int:
    stop_code = _cmd_stop(args)
    if stop_code != 0:
        return stop_code
    time.sleep(2.0)
    return _cmd_start(args)


def _cmd_dev_restart(args: argparse.Namespace) -> int:
    stop_code = _cmd_dev_stop(args)
    if stop_code != 0:
        return stop_code
    time.sleep(2.0)
    return _cmd_dev_start(args)


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


def _cmd_local_services(args: argparse.Namespace) -> int:
    from pycloud_parallel.controlplane.local_ipc import inspect_local_services

    rows = inspect_local_services(timeout_sec=float(getattr(args, "timeout_sec", 0.5) or 0.5))
    print("============================================")
    print("  Local Services")
    print("============================================")
    print()
    if not rows:
        print("  (no local services)")
        return 0
    for row in rows:
        service_name = str(row.get("service_name", "") or "")
        pid = int(row.get("pid", 0) or 0)
        alive = bool(row.get("alive", False))
        node_id = str(row.get("node_id", "") or "")
        node_instance_id = str(row.get("node_instance_id", "") or "")
        status = "RUNNING" if alive else "STALE"
        print(
            f"  {'*' if alive else '-'} {service_name} "
            f"pid={pid or '-'} node={node_id or '-'} instance={node_instance_id or '-'} {status}"
        )
        error = str(row.get("error", "") or "").strip()
        if error and not alive:
            print(f"      error: {error}")
    return 0


def _cmd_stop_local_service(args: argparse.Namespace) -> int:
    from pycloud_parallel.controlplane.local_ipc import stop_local_service

    service_name = str(getattr(args, "service_name", "") or "").strip()
    result = stop_local_service(
        service_name,
        timeout_sec=float(getattr(args, "timeout_sec", 3.0) or 3.0),
        force=bool(getattr(args, "force", False)),
    )
    if bool(result.get("stopped", False)):
        _log("OK", f"Stopped local service {service_name} (pid={result.get('pid') or '-'})")
        return 0
    _log("ERROR", f"Failed to stop local service {service_name}: {result.get('error') or 'still running'}")
    return 1


def _cmd_gc_local_services(args: argparse.Namespace) -> int:
    from pycloud_parallel.controlplane.local_ipc import cleanup_stale_local_services

    removed = cleanup_stale_local_services(timeout_sec=float(getattr(args, "timeout_sec", 0.5) or 0.5))
    if removed:
        for row in removed:
            _log("OK", f"Removed stale local service registry {row.get('service_name') or '-'} pid={row.get('pid') or '-'}")
    else:
        _log("OK", "No stale local service registry entries")
    return 0


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


def _segment_dir(object_dir: Path) -> Path:
    return object_dir / "segments"


def _load_json(path: Path) -> Dict[str, object]:
    try:
        return json.loads(path.read_text(encoding="utf-8") or "{}")
    except Exception:
        return {}


def _parse_iso_datetime(value: object, *, fallback: datetime | None = None) -> datetime:
    text = str(value or "").strip()
    if text:
        with contextlib.suppress(Exception):
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
    return fallback or datetime.now(timezone.utc)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _format_size_bytes(size_bytes: int) -> str:
    size = max(0, int(size_bytes or 0))
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{size} B"


def _remove_code_index_entry(artifact_dir: Path, *, code_version: str, entry_module: str, entry_callable: str) -> None:
    if not str(code_version or "").strip() or not str(entry_module or "").strip():
        return
    link_path = _code_index_link_path(
        artifact_dir,
        code_version=code_version,
        entry_module=entry_module,
        entry_callable=entry_callable,
    )
    meta_path = _code_index_meta_path(
        artifact_dir,
        code_version=code_version,
        entry_module=entry_module,
        entry_callable=entry_callable,
    )
    with contextlib.suppress(FileNotFoundError):
        meta_path.unlink()
    if link_path.is_symlink():
        with contextlib.suppress(FileNotFoundError):
            link_path.unlink()
        return
    if link_path.is_dir():
        shutil.rmtree(link_path, ignore_errors=True)
        return
    with contextlib.suppress(FileNotFoundError):
        link_path.unlink()


def _collect_code_cache_rows(
    artifact_dir: Path,
    *,
    match: str = "",
    limit: int = 0,
    ensure_index: bool = True,
) -> List[Dict[str, object]]:
    codes_dir = artifact_dir / "codes"
    rows: List[Dict[str, object]] = []
    pattern = str(match or "").strip().lower()
    if not codes_dir.exists():
        return rows

    for meta_path in sorted(codes_dir.glob("*/subversions/*/meta.json")):
        meta = _load_json(meta_path)
        code_version = str(meta.get("code_version", "") or "").strip()
        code_digest = code_version.removeprefix("sha256:").split(".", 1)[0].strip().lower()
        entry_module = str(meta.get("entry_module", "") or "").strip()
        entry_callable = str(meta.get("entry_callable", "") or "").strip()
        if not code_version or not code_digest or not entry_module:
            continue
        code_dir = meta_path.parent
        content_dir = meta_path.parents[2]
        if ensure_index:
            _ensure_code_index_entry(artifact_dir, code_version=code_version)
        index_path = _code_index_link_path(
            artifact_dir,
            code_version=code_version,
            entry_module=entry_module,
            entry_callable=entry_callable,
        )
        row = {
            "code_version": code_version,
            "code_digest": code_digest,
            "entry_module": entry_module,
            "entry_callable": entry_callable,
            "package_format": str(meta.get("package_format", "") or "").strip(),
            "runtime": str(meta.get("runtime", "") or "").strip(),
            "size_bytes": max(0, int(meta.get("size_bytes", 0) or 0)),
            "size_human": _format_size_bytes(int(meta.get("size_bytes", 0) or 0)),
            "created_at": str(meta.get("created_at", "") or "").strip(),
            "last_at": str(meta.get("last_at", "") or "").strip(),
            "artifact_path": str(meta.get("artifact_path", "") or "").strip(),
            "dependency_path": str(meta.get("dependency_path", "") or "").strip(),
            "data_path": str(meta.get("data_path", "") or "").strip(),
            "code_dir": str(code_dir),
            "content_dir": str(content_dir),
            "code_key": content_dir.name,
            "subversion_key": code_dir.name,
            "storage_key": code_dir.name,
            "index_path": str(index_path),
            "index_ready": bool(index_path.exists() or index_path.is_symlink()),
        }
        if pattern:
            haystack = "\n".join(
                [
                    row["entry_module"],
                    row["entry_callable"],
                    row["code_digest"],
                    row["code_key"],
                    row["subversion_key"],
                    row["code_version"],
                    row["artifact_path"],
                    row["dependency_path"],
                    row["data_path"],
                    row["code_dir"],
                    row["content_dir"],
                    row["index_path"],
                ]
            ).lower()
            if pattern not in haystack:
                continue
        rows.append(row)

    rows.sort(
        key=lambda item: (
            _parse_iso_datetime(item.get("last_at")).timestamp(),
            _parse_iso_datetime(item.get("created_at")).timestamp(),
            str(item.get("entry_module", "")),
        ),
        reverse=True,
    )
    if limit > 0:
        rows = rows[:limit]
    return rows


def _cmd_cache_list(args: argparse.Namespace) -> int:
    root = _runtime_root(args.runtime_root)
    artifact_dir = Path(args.artifact_dir).expanduser().resolve() if args.artifact_dir else _default_artifact_dir(root)
    rows = _collect_code_cache_rows(
        artifact_dir,
        match=str(getattr(args, "match", "") or ""),
        limit=max(0, int(getattr(args, "limit", 0) or 0)),
        ensure_index=True,
    )
    if bool(getattr(args, "json", False)):
        payload = {
            "ok": True,
            "artifact_dir": str(artifact_dir),
            "index_dir": str(artifact_dir / "code_index"),
            "match": str(getattr(args, "match", "") or ""),
            "count": len(rows),
            "items": rows,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    print("============================================")
    print("  Code Cache")
    print("============================================")
    print()
    print(f"  Artifact Dir: {artifact_dir}")
    print(f"  Index Dir: {artifact_dir / 'code_index'}")
    if str(getattr(args, 'match', '') or '').strip():
        print(f"  Match: {str(getattr(args, 'match', '') or '').strip()}")
    print(f"  Count: {len(rows)}")
    print()
    if not rows:
        print("  (no cached code artifacts)")
        return 0

    for idx, row in enumerate(rows, start=1):
        callable_name = str(row["entry_callable"] or "").strip() or "<module>"
        print(f"  [{idx}] {row['entry_module']}:{callable_name}")
        print(
            f"      index: {row['index_path']}"
            f"{'' if row['index_ready'] else ' (index link unavailable on this host)'}"
        )
        print(f"      target: {row['code_dir']}")
        print(
            f"      format: {row['package_format'] or '-'}"
            f"  size: {row['size_human']}"
            f"  last_at: {row['last_at'] or '-'}"
        )
        print(f"      code_digest: {row['code_digest']}")
        print(f"      code_key: {row['code_key']}  subversion_key: {row['subversion_key']}")
        print(f"      code_version: {row['code_version']}")
    return 0


def _collect_current_globals_object_ids(artifact_dir: Path) -> set[str]:
    live: set[str] = set()
    codes_dir = artifact_dir / "codes"
    if not codes_dir.exists():
        return live
    for current_path in codes_dir.glob("*/subversions/*/globals/*/*/current.json"):
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

            stack = [value_payload]
            while stack:
                value = stack.pop()
                if isinstance(value, dict):
                    ref = maybe_data_ref(value)
                    if ref is not None:
                        try:
                            object_id = normalize_object_id(ref.object_id)
                        except ValueError:
                            continue
                        live.add(object_id)
                        continue
                    stack.extend(value.values())
                    continue
                if isinstance(value, list):
                    stack.extend(value)
    return live


def _collect_active_data_ref_object_ids(target: str) -> set[str]:
    normalized_target = str(target or "").strip()
    if not normalized_target:
        return set()
    try:
        from pycloud_parallel.controlplane.infocenter_client import InfoCenterClient

        with InfoCenterClient(normalized_target, timeout_sec=3.0) as client:
            refs = list(client.list_data_refs(limit=5000))
    except Exception:
        return set()

    live: set[str] = set()
    for item in refs:
        if not isinstance(item, dict):
            continue
        raw_id = str(item.get("storage_id", "") or item.get("ref_id", "") or "").strip()
        if not raw_id:
            continue
        try:
            live.add(normalize_object_id(raw_id))
        except ValueError:
            continue
    return live


def _object_backing_path_from_meta(object_dir: Path, *, object_id: str, meta: Dict[str, object]) -> Path | None:
    normalized_id = normalize_object_id(object_id)
    digest = normalized_id.replace("sha256:", "", 1)
    fmt = normalize_object_format(str(meta.get("format", "") or "bin"), default="bin")
    storage_backend = str(meta.get("storage_backend", "file") or "file").strip() or "file"
    if storage_backend == "segment":
        relpath = str(meta.get("segment_relpath", "") or "").strip()
        return (_segment_dir(object_dir).parent / relpath).resolve() if relpath else None
    candidates = [
        object_storage_path(object_dir, object_id=normalized_id, fmt=fmt),
        object_dir / f"{digest}{object_format_suffix(fmt)}",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    subdir = object_dir / digest[:2]
    if subdir.exists():
        matches = sorted(path for path in subdir.glob(f"{digest[2:]}*") if path.is_file())
        if matches:
            return matches[0]
    matches = sorted(path for path in object_dir.glob(f"{digest}*") if path.is_file())
    if matches:
        return matches[0]
    return candidates[0]


def _segment_ref_rows_from_meta(object_dir: Path, meta_dir: Path) -> Dict[str, List[Dict[str, object]]]:
    out: Dict[str, List[Dict[str, object]]] = {}
    for meta_path in sorted(meta_dir.glob("*.json")):
        meta = _load_json(meta_path)
        if str(meta.get("storage_backend", "file") or "file").strip() != "segment":
            continue
        object_id = str(meta.get("object_id", "") or "").strip()
        relpath = str(meta.get("segment_relpath", "") or "").strip()
        if not object_id or not relpath:
            continue
        row = {
            "object_id": object_id,
            "meta_path": meta_path,
            "segment_relpath": relpath,
            "segment_offset": max(0, int(meta.get("segment_offset", 0) or 0)),
            "segment_length": max(0, int(meta.get("segment_length", meta.get("size_bytes", 0)) or 0)),
            "size_bytes": max(0, int(meta.get("size_bytes", 0) or 0)),
        }
        out.setdefault(relpath, []).append(row)
    for rows in out.values():
        rows.sort(key=lambda item: (int(item["segment_offset"]), str(item["object_id"])))
    return out


def _segment_compaction_plan(segment_path: Path, refs: List[Dict[str, object]]) -> Dict[str, object]:
    live_bytes = sum(int(item.get("segment_length", 0) or 0) for item in refs)
    file_size = int(segment_path.stat().st_size)
    current_offset = 0
    already_compact = live_bytes == file_size
    if already_compact:
        for item in refs:
            length = int(item.get("segment_length", 0) or 0)
            offset = int(item.get("segment_offset", 0) or 0)
            if offset != current_offset:
                already_compact = False
                break
            current_offset += length
    return {
        "file_size": file_size,
        "live_bytes": live_bytes,
        "wasted_bytes": max(0, file_size - live_bytes),
        "needs_compaction": bool(refs) and not already_compact,
    }


def _compact_segment_file(object_dir: Path, *, relpath: str, refs: List[Dict[str, object]]) -> Dict[str, object]:
    segment_path = (object_dir / relpath).resolve()
    plan = _segment_compaction_plan(segment_path, refs)
    if not plan["needs_compaction"]:
        return {
            "segment_relpath": relpath,
            "path": str(segment_path),
            **plan,
            "compacted": False,
        }
    tmp_path = segment_path.with_suffix(segment_path.suffix + ".tmp")
    current_offset = 0
    with segment_path.open("rb") as src, tmp_path.open("wb") as dst:
        for ref in refs:
            src.seek(int(ref["segment_offset"]))
            blob = src.read(int(ref["segment_length"]))
            if len(blob) != int(ref["segment_length"]):
                raise RuntimeError(f"segment compaction failed: truncated read for {ref['object_id']}")
            dst.write(blob)
            meta = _load_json(ref["meta_path"])
            meta["segment_offset"] = current_offset
            meta["segment_length"] = int(ref["segment_length"])
            _write_json(ref["meta_path"], meta)
            current_offset += int(ref["segment_length"])
    os.replace(tmp_path, segment_path)
    return {
        "segment_relpath": relpath,
        "path": str(segment_path),
        **plan,
        "compacted": True,
    }


def _cmd_gc(args: argparse.Namespace) -> int:
    root = _runtime_root(args.runtime_root)
    artifact_dir = Path(args.artifact_dir).expanduser().resolve() if args.artifact_dir else _default_artifact_dir(root)
    if not bool(getattr(args, "dry_run", False)) and not bool(getattr(args, "force", False)):
        running = _running_managed_processes(root)
        if running:
            names = ", ".join(f"{name}(pid={pid})" for name, pid in running)
            print(
                json.dumps(
                    {
                        "ok": False,
                        "error": "gc refused while managed processes are running",
                        "runtime_root": str(root),
                        "artifact_dir": str(artifact_dir),
                        "running_processes": [
                            {"name": name, "pid": pid}
                            for name, pid in running
                        ],
                        "hint": "Stop local pycloud processes first, or re-run gc with --dry-run to inspect or --force to override.",
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 2
    object_dir = artifact_dir / "objects"
    meta_dir = _object_meta_dir(object_dir)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, int(args.older_than_hours)))
    deleted_objects: List[Dict[str, object]] = []
    kept_objects: List[Dict[str, object]] = []
    deleted_codes: List[Dict[str, object]] = []
    kept_codes: List[Dict[str, object]] = []
    deleted_segments: List[Dict[str, object]] = []
    kept_segments: List[Dict[str, object]] = []
    compacted_segments: List[Dict[str, object]] = []
    active_registry_object_ids = _collect_active_data_ref_object_ids(str(getattr(args, "target", "") or "").strip())

    if args.scope in ("codes", "all"):
        codes_dir = artifact_dir / "codes"
        if codes_dir.exists():
            for code_dir in sorted(path for path in codes_dir.iterdir() if path.is_dir()):
                variant_meta_paths = sorted(code_dir.glob("subversions/*/meta.json"))
                variant_metas = [_load_json(path) for path in variant_meta_paths]
                timestamps: List[datetime] = []
                for meta in variant_metas:
                    last_at_raw = str(meta.get("last_at", "") or "").strip()
                    try:
                        last_at = datetime.fromisoformat(last_at_raw)
                        if last_at.tzinfo is None:
                            last_at = last_at.replace(tzinfo=timezone.utc)
                    except Exception:
                        continue
                    timestamps.append(last_at.astimezone(timezone.utc))
                if timestamps:
                    last_at = max(timestamps)
                else:
                    last_at = datetime.fromtimestamp(code_dir.stat().st_mtime, tz=timezone.utc)
                row = {
                    "code_digest": code_dir.name,
                    "path": str(code_dir),
                    "last_at": last_at.astimezone(timezone.utc).isoformat(),
                }
                if last_at >= cutoff:
                    row["reason"] = "recently_used"
                    kept_codes.append(row)
                    continue
                deleted_codes.append(row)
                if not args.dry_run:
                    for meta in variant_metas:
                        _remove_code_index_entry(
                            artifact_dir,
                            code_version=str(meta.get("code_version", "") or "").strip(),
                            entry_module=str(meta.get("entry_module", "") or "").strip(),
                            entry_callable=str(meta.get("entry_callable", "") or "").strip(),
                        )
                    shutil.rmtree(code_dir, ignore_errors=True)

    if args.scope in ("objects", "all") and object_dir.exists():
        live_object_ids = _collect_current_globals_object_ids(artifact_dir)
        for meta_path in sorted(meta_dir.glob("*.json")) if meta_dir.exists() else []:
            meta = _load_json(meta_path)
            object_id = str(meta.get("object_id", "") or "").strip()
            if not object_id:
                continue
            try:
                object_id = normalize_object_id(object_id)
            except Exception:
                continue
            backing_path = _object_backing_path_from_meta(object_dir, object_id=object_id, meta=meta)
            last_at_raw = str(meta.get("last_at", "") or "").strip()
            try:
                last_at = datetime.fromisoformat(last_at_raw)
                if last_at.tzinfo is None:
                    last_at = last_at.replace(tzinfo=timezone.utc)
            except Exception:
                if backing_path is not None and backing_path.exists():
                    last_at = datetime.fromtimestamp(backing_path.stat().st_mtime, tz=timezone.utc)
                else:
                    last_at = datetime.now(timezone.utc)
            storage_backend = str(meta.get("storage_backend", "file") or "file").strip() or "file"

            row = {
                "object_id": object_id,
                "path": str(backing_path) if backing_path is not None else "",
                "storage_backend": storage_backend,
                "size_bytes": int(meta.get("size_bytes", 0) or (backing_path.stat().st_size if backing_path is not None and backing_path.exists() else 0)),
                "last_at": last_at.astimezone(timezone.utc).isoformat(),
            }
            if object_id in live_object_ids:
                row["reason"] = "referenced_by_current_globals"
                kept_objects.append(row)
                continue
            if object_id in active_registry_object_ids:
                row["reason"] = "referenced_by_active_data_ref"
                kept_objects.append(row)
                continue
            if last_at >= cutoff:
                row["reason"] = "recently_used"
                kept_objects.append(row)
                continue
            deleted_objects.append(row)
            if not args.dry_run:
                with contextlib.suppress(FileNotFoundError):
                    meta_path.unlink()
                if storage_backend != "segment" and backing_path is not None:
                    with contextlib.suppress(FileNotFoundError):
                        backing_path.unlink()

        segment_refs = _segment_ref_rows_from_meta(object_dir, meta_dir)
        segments_root = _segment_dir(object_dir)
        if segments_root.exists():
            for segment_path in sorted(path for path in segments_root.glob("**/*") if path.is_file()):
                relpath = str(segment_path.resolve().relative_to(object_dir.resolve()))
                refs = segment_refs.get(relpath, [])
                if not refs:
                    row = {
                        "segment_relpath": relpath,
                        "path": str(segment_path),
                        "size_bytes": int(segment_path.stat().st_size),
                        "reason": "orphan_segment",
                    }
                    deleted_segments.append(row)
                    if not args.dry_run:
                        with contextlib.suppress(FileNotFoundError):
                            segment_path.unlink()
                    continue
                plan = _segment_compaction_plan(segment_path, refs)
                segment_row = {
                    "segment_relpath": relpath,
                    "path": str(segment_path),
                    "size_bytes": int(segment_path.stat().st_size),
                    "live_bytes": int(plan["live_bytes"]),
                    "wasted_bytes": int(plan["wasted_bytes"]),
                }
                if plan["needs_compaction"] and (not args.dry_run):
                    compacted = _compact_segment_file(object_dir, relpath=relpath, refs=refs)
                    compacted_segments.append(compacted)
                    segment_row["reason"] = "compacted"
                    kept_segments.append(segment_row)
                    continue
                if plan["needs_compaction"]:
                    segment_row["reason"] = "needs_compaction"
                else:
                    segment_row["reason"] = "in_use"
                kept_segments.append(segment_row)

    payload = {
        "ok": True,
        "artifact_dir": str(artifact_dir),
        "dry_run": bool(args.dry_run),
        "scope": args.scope,
        "older_than_hours": int(args.older_than_hours),
        "target": str(getattr(args, "target", "") or "").strip(),
        "deleted_objects": deleted_objects,
        "kept_objects": kept_objects,
        "deleted_segments": deleted_segments,
        "kept_segments": kept_segments,
        "compacted_segments": compacted_segments,
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


def _add_local_argument(parser: argparse.ArgumentParser, *, dest: str = "local") -> None:
    parser.add_argument(
        "--loopback",
        "--local",
        dest=dest,
        action="store_true",
        default=argparse.SUPPRESS,
        help='bind managed HTTP services to 127.0.0.1 by default; unrelated to target="local" local IPC',
    )


def _add_debug_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--debug",
        dest="debug",
        action="store_true",
        default=False,
        help=(
            "debug launch mode: spawned main service processes use DEBUG log level and attach stdio to a visible "
            "console/terminal (Windows keeps dedicated process windows; POSIX inherits the current terminal)"
        ),
    )


def _add_dev_node_arguments(parser: argparse.ArgumentParser, *, restart: bool = False) -> None:
    parser.add_argument(
        "--nodes",
        type=int,
        default=2,
        help=(
            "number of local node control processes to start after restart; supports 0, 1, or N"
            if restart
            else "number of local node control processes to start; supports 0, 1, or N"
        ),
    )
    parser.add_argument("--node-host", default="", help="bind host used by dev node control endpoints; default auto-detects local IP")
    parser.add_argument(
        "--node-control-port",
        type=int,
        default=50061,
        help="base bind port for dev node control endpoints; node-N increments from this value",
    )
    parser.add_argument(
        "--node-service-http-host",
        default="",
        help="bind host used by dev node service HTTP endpoints; default auto-detects local IP",
    )
    parser.add_argument(
        "--node-service-http-port",
        type=int,
        default=18081,
        help="base bind port for dev node service HTTP endpoints; node-N increments from this value",
    )
    parser.add_argument(
        "--node-worker-capacity",
        type=int,
        default=0,
        help="worker capacity per dev node; default auto-calculated from CPU or PYCLOUD_NODE_WORKER_CAPACITY",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = _CtlArgumentParser(description="PyCloud local service manager")
    parser.set_defaults(local=False, _global_local=False)
    parser.add_argument("--runtime-root", default="", help="base directory for logs and pid files (default: cwd or PYCLOUD_HOME)")
    _add_local_argument(parser, dest="_global_local")
    parser.add_argument("--controlplane-host", default="", help="bind host used by `pycloudctl start` for controlplane; default auto-detects local IP")
    parser.add_argument("--controlplane-port", type=int, default=50051, help="bind port used by `pycloudctl start` for controlplane")
    parser.add_argument("--job-orchestrator-host", default="", help="bind host used by `pycloudctl start` for job-orchestrator; default auto-detects local IP")
    parser.add_argument("--job-orchestrator-port", type=int, default=50053, help="bind port used by `pycloudctl start` for job-orchestrator")

    subparsers = parser.add_subparsers(dest="command", required=True)
    start_parser = subparsers.add_parser("start", help="start controlplane and job-orchestrator")
    _add_local_argument(start_parser)
    _add_env_argument(start_parser)
    _add_debug_argument(start_parser)
    dev_start_parser = subparsers.add_parser("dev-start", help="start controlplane, job-orchestrator, and a configurable local node set")
    _add_local_argument(dev_start_parser)
    _add_env_argument(dev_start_parser)
    _add_debug_argument(dev_start_parser)
    _add_dev_node_arguments(dev_start_parser)
    start_infocenter = subparsers.add_parser("start-infocenter", help="start one local infocenter process")
    _add_local_argument(start_infocenter)
    _add_env_argument(start_infocenter)
    _add_debug_argument(start_infocenter)
    start_infocenter.add_argument("--bind", default="0.0.0.0:50051", help="full bind address in host:port form for start-infocenter; wildcard hosts auto-resolve to the local IP")
    start_gateway = subparsers.add_parser("start-gateway", help="start one local gateway process")
    _add_local_argument(start_gateway)
    _add_env_argument(start_gateway)
    _add_debug_argument(start_gateway)
    start_gateway.add_argument("--bind", default="0.0.0.0:50052", help="full bind address in host:port form for start-gateway; wildcard hosts auto-resolve to the local IP")
    start_gateway.add_argument(
        "--target",
        "--infocenter-addr",
        dest="infocenter_addr",
        default="",
        help='InfoCenter/ControlPlane target; use "local" only for commands that explicitly support local IPC',
    )
    start_gateway.add_argument("--gateway-refresh-interval-sec", type=float, default=3.0, help="gateway route refresh interval in seconds")
    start_gateway.add_argument("--gateway-failure-threshold", type=int, default=3, help="circuit-breaker failure threshold for gateway route refresh")
    start_gateway.add_argument("--gateway-open-sec", type=float, default=5.0, help="circuit-breaker open duration in seconds for gateway route refresh")
    start_controlplane = subparsers.add_parser("start-controlplane", help="start one local controlplane process")
    _add_local_argument(start_controlplane)
    _add_env_argument(start_controlplane)
    _add_debug_argument(start_controlplane)
    start_controlplane.add_argument("--bind", default="0.0.0.0:50051", help="full bind address in host:port form for start-controlplane; wildcard hosts auto-resolve to the local IP")
    start_controlplane.add_argument("--gateway-refresh-interval-sec", type=float, default=3.0, help="gateway route refresh interval in seconds for embedded gateway state")
    start_controlplane.add_argument("--gateway-failure-threshold", type=int, default=3, help="circuit-breaker failure threshold for embedded gateway state")
    start_controlplane.add_argument("--gateway-open-sec", type=float, default=5.0, help="circuit-breaker open duration in seconds for embedded gateway state")
    start_job_orchestrator = subparsers.add_parser("start-job-orchestrator", help="start one local job-orchestrator process")
    _add_local_argument(start_job_orchestrator)
    _add_env_argument(start_job_orchestrator)
    _add_debug_argument(start_job_orchestrator)
    start_job_orchestrator.add_argument("--bind", default="0.0.0.0:50053", help="full bind address in host:port form for start-job-orchestrator; wildcard hosts auto-resolve to the local IP")
    start_job_orchestrator.add_argument(
        "--target",
        "--infocenter-addr",
        dest="infocenter_addr",
        default="",
        help='InfoCenter/ControlPlane target; "local" starts the job-orchestrator as a local IPC service',
    )
    start_job_orchestrator.add_argument("--node-id", default="job-orchestrator-01", type=_normalize_managed_name, help="managed node id advertised by job-orchestrator")
    start_job_orchestrator.add_argument("--service-name", default="job-orchestrator", help="service name registered by job-orchestrator")
    start_job_orchestrator.add_argument("--queue-capacity", type=int, default=NODE_QUEUE_CAPACITY, help="queue capacity advertised by job-orchestrator")
    start_job_orchestrator.add_argument("--node-tags", default="job", help="comma-separated node tags advertised by job-orchestrator")
    start_job_orchestrator.add_argument("--node-version", default="v1", help="node version string advertised by job-orchestrator")
    start_job_orchestrator.add_argument("--force", action="store_true", help="replace an existing local IPC job-orchestrator with the same service name when --target local")
    start_job_orchestrator.add_argument("--admin-token", default="", help="admin token required for reorder_job; defaults to PYCLOUD_JOB_ORCHESTRATOR_ADMIN_TOKEN or PYCLOUD_INFOCENTER_TOKEN")
    start_node = subparsers.add_parser("start-node", help="start one local node control process")
    _add_local_argument(start_node)
    _add_env_argument(start_node)
    _add_debug_argument(start_node)
    start_node.add_argument("--node-id", default="node-local-01", type=_normalize_managed_name, help="managed node name used for pid/log files and registration")
    start_node.add_argument("--bind", default="0.0.0.0:50061", help="full HTTP control bind address in host:port form for start-node; wildcard hosts auto-resolve to the local IP")
    start_node.add_argument("--service-http-bind", default="", help="full service HTTP bind address in host:port form for start-node; defaults to the node bind host and a port derived from the control port, for example 50061 -> 18081")
    start_node.add_argument(
        "--target",
        "--infocenter-addr",
        dest="infocenter_addr",
        default=None,
        help='InfoCenter/ControlPlane target for registration; pass empty string ("") to disable registration',
    )
    start_node.add_argument("--advertise-addr", default="", help="full advertised control address in host:port form; defaults to the auto-resolved HTTP control bind address")
    start_node.add_argument("--worker-capacity", type=int, default=0, help="node runtime worker capacity; 0 means auto-calculate")
    start_node.add_argument("--queue-capacity", type=int, default=NODE_QUEUE_CAPACITY, help="node task queue capacity")
    start_node.add_argument("--max-workers", type=int, default=NODE_MAX_WORKERS, help="max HTTP control server worker threads for this node")
    start_node.add_argument("--service-default-workers", type=int, default=SERVICE_DEFAULT_WORKERS, help="default worker count for deployed services on this node")
    start_node.add_argument("--service-heartbeat-timeout-sec", type=int, default=SERVICE_HEARTBEAT_TIMEOUT_SEC, help="default heartbeat timeout in seconds for deployed services on this node")
    start_node.add_argument("--node-tags", default="compute", help="comma-separated node tags advertised during registration")
    start_node.add_argument("--node-version", default="v1", help="node version string advertised during registration")
    stop_parser = subparsers.add_parser("stop", help="stop local core controlplane and job-orchestrator services")
    stop_parser.add_argument("--target", default="", help="InfoCenter/ControlPlane target for best-effort node cleanup before stop")
    stop_parser.add_argument("--scan-ports", action="store_true", help="after pid-based stop, scan configured ports and stop matching pycloud listener processes")
    stop_parser.add_argument("--ports", default="", help="comma-separated ports for doctor/scan-ports; default uses controlplane/node HTTP ports")
    stopall_parser = subparsers.add_parser("stopall", help="stop all local pycloud managed/server/local IPC processes")
    stopall_parser.add_argument("--target", default="", help="InfoCenter/ControlPlane target for best-effort node cleanup before stop")
    stopall_parser.add_argument("--scan-ports", action="store_true", help="after process stop, scan configured ports and stop matching pycloud listener processes")
    stopall_parser.add_argument("--ports", default="", help="comma-separated ports for scan-ports; default uses controlplane/node HTTP ports")
    stopall_parser.add_argument("--timeout-sec", type=float, default=3.0, help="local IPC service shutdown timeout")
    dev_stop_parser = subparsers.add_parser("dev-stop", help="stop local dev profile services")
    dev_stop_parser.add_argument("--target", default="", help="InfoCenter/ControlPlane target for best-effort node cleanup before stop")
    stop_node_parser = subparsers.add_parser("stop-node", help="stop one local node control process")
    stop_node_parser.add_argument("node_name", type=_normalize_managed_name, help="node control process name to stop")
    stop_node_parser.add_argument("--target", default="", help="InfoCenter/ControlPlane target for best-effort node cleanup before stop")
    restart_parser = subparsers.add_parser("restart", help="restart local core controlplane and job-orchestrator services")
    _add_local_argument(restart_parser)
    _add_env_argument(restart_parser)
    _add_debug_argument(restart_parser)
    dev_restart_parser = subparsers.add_parser("dev-restart", help="restart local dev profile services")
    _add_local_argument(dev_restart_parser)
    _add_env_argument(dev_restart_parser)
    _add_debug_argument(dev_restart_parser)
    _add_dev_node_arguments(dev_restart_parser, restart=True)
    status_parser = subparsers.add_parser("status", help="show local service status")
    status_parser.add_argument("--target", default="", help="InfoCenter/ControlPlane target for node/service query (default: 127.0.0.1:<controlplane-port>)")
    local_services_parser = subparsers.add_parser("local-services", help="list Service.startup(target='local') IPC services")
    local_services_parser.add_argument("--timeout-sec", type=float, default=0.5, help="per-service IPC ping timeout")
    stop_local_parser = subparsers.add_parser("stop-local-service", help="stop one Service.startup(target='local') IPC service")
    stop_local_parser.add_argument("service_name", help="local service_name to stop")
    stop_local_parser.add_argument("--timeout-sec", type=float, default=3.0, help="shutdown timeout before returning")
    stop_local_parser.add_argument("--force", action="store_true", help="force kill if graceful IPC shutdown does not stop the process")
    gc_local_parser = subparsers.add_parser("gc-local-services", help="remove stale local service IPC registry entries")
    gc_local_parser.add_argument("--timeout-sec", type=float, default=0.5, help="per-service IPC ping timeout")
    doctor_parser = subparsers.add_parser("doctor", help="inspect runtime-root pid files and listener ports")
    doctor_parser.add_argument("--target", default="", help="InfoCenter/ControlPlane target for context display (default: 127.0.0.1:<controlplane-port>)")
    doctor_parser.add_argument("--ports", default="", help="comma-separated ports to inspect; default uses controlplane/node HTTP ports")
    cache_list_parser = subparsers.add_parser("cache-list", help="list cached code artifacts and readable index paths")
    cache_list_parser.add_argument("--artifact-dir", default="", help="artifact/code cache directory (default: <runtime-root>/code_cache)")
    cache_list_parser.add_argument("--match", default="", help="substring filter for module/callable/code_version/path")
    cache_list_parser.add_argument("--limit", type=int, default=50, help="max rows to print; 0 means all")
    cache_list_parser.add_argument("--json", action="store_true", help="print JSON instead of human-readable text")
    gc_parser = subparsers.add_parser("gc", help="garbage collect cached object files")
    gc_parser.add_argument("--artifact-dir", default="", help="artifact/code cache directory (default: <runtime-root>/code_cache)")
    gc_parser.add_argument("--target", default="", help="InfoCenter/ControlPlane target used to preserve active DataRef-backed objects during gc")
    gc_parser.add_argument("--scope", choices=["codes", "objects", "all"], default="all")
    gc_parser.add_argument("--older-than-hours", type=int, default=24 * 7)
    gc_parser.add_argument("--dry-run", action="store_true")
    gc_parser.add_argument("--force", action="store_true", help="allow destructive gc even if managed local processes are still running")
    parser.epilog = (
        "Environment overrides:\n"
        "  start / dev-start / start-infocenter / start-gateway / start-controlplane / start-job-orchestrator / start-node / restart / dev-restart\n"
        "  support repeated '--env KEY=VALUE' arguments. Example:\n"
        "    pycloudctl start --env PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES=1048576"
    )
    return parser


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "start":
        return _cmd_start(args)
    if args.command == "dev-start":
        return _cmd_dev_start(args)
    if args.command == "start-infocenter":
        return _cmd_start_infocenter(args)
    if args.command == "start-gateway":
        return _cmd_start_gateway(args)
    if args.command == "start-controlplane":
        return _cmd_start_controlplane(args)
    if args.command == "start-job-orchestrator":
        return _cmd_start_job_orchestrator(args)
    if args.command == "start-node":
        return _cmd_start_node(args)
    if args.command == "stop":
        return _cmd_stop(args)
    if args.command == "stopall":
        return _cmd_stopall(args)
    if args.command == "dev-stop":
        return _cmd_dev_stop(args)
    if args.command == "stop-node":
        return _cmd_stop_node(args)
    if args.command == "restart":
        return _cmd_restart(args)
    if args.command == "dev-restart":
        return _cmd_dev_restart(args)
    if args.command == "status":
        return _cmd_status(args)
    if args.command == "local-services":
        return _cmd_local_services(args)
    if args.command == "stop-local-service":
        return _cmd_stop_local_service(args)
    if args.command == "gc-local-services":
        return _cmd_gc_local_services(args)
    if args.command == "doctor":
        return _cmd_doctor(args)
    if args.command == "cache-list":
        return _cmd_cache_list(args)
    if args.command == "gc":
        return _cmd_gc(args)
    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
