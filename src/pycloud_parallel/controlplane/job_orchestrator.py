from __future__ import annotations

import html
import importlib
import json
import logging
import os
import threading
import uuid
from typing import Dict, List, Optional, Tuple

from pycloud_parallel.controlplane.job_queue import JobQueueManager
from pycloud_parallel.controlplane.policy_profile import get_default_policy_id_for_binding
from pycloud_parallel.controlplane.startup_service_node import StartupServiceNode


DEFAULT_JOB_ORCHESTRATOR_SERVICE_NAME = "job-orchestrator"
_JOB_ORCH_SUBMIT_TRANSPORT_MODE = "structured_v1"
_SUBMIT_POLICY_FIELD_ERROR = (
    "job submit policy_id/taskpool_policy_id is not supported; "
    "policy is owned by startup node/deployment"
)
logger = logging.getLogger(__name__)


def _reject_submit_policy_fields(payload: Dict[str, object]) -> Optional[Tuple[int, Dict[str, object]]]:
    for field in ("policy_id", "taskpool_policy_id"):
        if str((payload or {}).get(field, "") or "").strip():
            return 400, {"ok": False, "error": _SUBMIT_POLICY_FIELD_ERROR}
    return None


class JobOrchestratorModule:
    """Job queue service logic mounted by a startup-only service node."""

    def __init__(
        self,
        *,
        service_name: str = DEFAULT_JOB_ORCHESTRATOR_SERVICE_NAME,
        queue_capacity: int = 4000,
        taskpool_policy_id: str = "",
        admin_token: str = "",
        render_job_detail_page=None,
    ) -> None:
        self.service_name = str(service_name or DEFAULT_JOB_ORCHESTRATOR_SERVICE_NAME).strip() or DEFAULT_JOB_ORCHESTRATOR_SERVICE_NAME
        self.queue_capacity = max(1, int(queue_capacity or 1))
        self.taskpool_policy_id = (
            str(taskpool_policy_id or "").strip().lower()
            or get_default_policy_id_for_binding("taskpool_default")
        )
        self.admin_token = str(admin_token or "").strip()
        self.service_id = uuid.uuid4().hex
        self.base_url = ""
        self.job_queue = JobQueueManager(taskpool_policy_id=self.taskpool_policy_id)
        self._render_job_detail_page = render_job_detail_page

    def start(self, *, controlplane_target: str) -> None:
        self.job_queue.start(controlplane_target=controlplane_target)

    def close(self) -> None:
        self.job_queue.close()

    def _check_admin_token(self, token: str) -> bool:
        expected = str(self.admin_token or "").strip()
        if not expected:
            return False
        return str(token or "").strip() == expected

    def submit_job(
        self,
        payload: Dict[str, object],
        token: str,
        serialization_mode: str = "",
    ) -> Tuple[int, Dict[str, object]]:
        if str(serialization_mode or "").strip().lower() != _JOB_ORCH_SUBMIT_TRANSPORT_MODE:
            return 400, {
                "ok": False,
                "error": (
                    "job submit transport serialization_mode must be structured_v1; "
                    "use task_serialization_mode for TaskPool execution mode"
                ),
            }
        policy_rejected = _reject_submit_policy_fields(dict(payload or {}))
        if policy_rejected is not None:
            return policy_rejected
        logger.info(
            "[JobOrch] submit_job service_id=%s client_id=%s entry_module=%s job_mode=%s task_mode=%s reset_pool=%s",
            self.service_id,
            str((payload or {}).get("client_id", "") or ""),
            str((payload or {}).get("entry_module", "") or ""),
            str((payload or {}).get("job_mode", "") or ""),
            str((payload or {}).get("task_serialization_mode", "") or ""),
            bool((payload or {}).get("reset_pool", False)),
        )
        state = self.job_queue.submit_job(dict(payload or {}), auth_token=token)
        return 200, {"ok": True, "job": state.as_dict()}

    def get_job_status(self, job_id: str, *, include_details: bool = False) -> Tuple[int, Dict[str, object]]:
        normalized = str(job_id or "").strip()
        if not normalized:
            return 400, {"ok": False, "error": "job_id is required"}
        state = self.job_queue.get_job(normalized)
        if state is None:
            return 404, {"ok": False, "error": "job not found"}
        return 200, {
            "ok": True,
            "job": state.as_dict(
                include_payload=include_details,
                include_results=include_details,
                include_final_result=include_details,
            ),
        }

    def cancel_job(self, job_id: str, *, token: str) -> Tuple[int, Dict[str, object]]:
        normalized = str(job_id or "").strip()
        if not normalized:
            return 400, {"ok": False, "error": "job_id is required"}
        logger.info("[JobOrch] cancel_job service_id=%s job_id=%s", self.service_id, normalized)
        try:
            state = self.job_queue.cancel_job(normalized, auth_token=token)
        except PermissionError as exc:
            return 403, {"ok": False, "error": str(exc)}
        if state is None:
            return 404, {"ok": False, "error": "job not found"}
        return 200, {"ok": True, "job": state.as_dict()}

    def reorder_job(self, job_id: str, *, direction: str, token: str) -> Tuple[int, Dict[str, object]]:
        normalized = str(job_id or "").strip()
        if not normalized:
            return 400, {"ok": False, "error": "job_id is required"}
        if not self._check_admin_token(token):
            return 403, {"ok": False, "error": "admin auth required"}
        try:
            state = self.job_queue.reorder_job(normalized, direction=str(direction or "").strip().lower())
        except ValueError as exc:
            return 400, {"ok": False, "error": str(exc)}
        if state is None:
            return 404, {"ok": False, "error": "job not found"}
        if state.status != "WAITING":
            return 409, {"ok": False, "error": "only waiting jobs can be reordered", "job": state.as_dict()}
        return 200, {"ok": True, "job": state.as_dict(), "queue": self.job_queue.summary()}

    def extra_get(
        self,
        path_parts: List[str],
        query: Dict[str, List[str]],
    ) -> Optional[Tuple[int, Dict[str, object]]]:
        if len(path_parts) == 2 and path_parts[0] == "jobs":
            job_id = str(path_parts[1] or "").strip()
            if not job_id:
                return 400, {"ok": False, "error": "job_id is required"}
            state = self.job_queue.get_job(job_id)
            if state is None:
                return 404, {"ok": False, "error": "job not found"}
            view = str((query.get("view", [""]) or [""])[0] or "").strip().lower()
            if view == "html" and self._render_job_detail_page is not None:
                return 200, self._render_job_detail_page(state.as_dict()), "text/html; charset=utf-8"
            return 200, {"ok": True, "job": state.as_dict()}
        return None


class JobOrchestratorServer(StartupServiceNode):
    def __init__(
        self,
        *,
        bind: str,
        infocenter_addr: str,
        node_id: str = "job-orchestrator-01",
        service_name: str = DEFAULT_JOB_ORCHESTRATOR_SERVICE_NAME,
        queue_capacity: int = 4000,
        tags: Optional[List[str]] = None,
        version: str = "",
        job_orch_policy_id: str = "",
        taskpool_policy_id: str = "",
        admin_token: str = "",
    ) -> None:
        self.bind = str(bind or "").strip()
        self.infocenter_addr = str(infocenter_addr or "").strip()
        self.node_id = str(node_id or "job-orchestrator-01").strip() or "job-orchestrator-01"
        super().__init__(
            node_id=self.node_id,
            service_http_bind=self.bind,
            service_http_base_url="",
            worker_capacity=1,
            queue_capacity=queue_capacity,
            service_worker_capacity=1,
            task_pool_worker_capacity=1,
            enable_internal_executor=False,
            enable_service_session=False,
            accept_service_deploy=False,
        )
        self.service_name = str(service_name or DEFAULT_JOB_ORCHESTRATOR_SERVICE_NAME).strip() or DEFAULT_JOB_ORCHESTRATOR_SERVICE_NAME
        self.queue_capacity = max(1, int(queue_capacity or 1))
        self.tags = list(tags or ["job"])
        self.version = str(version or "")
        self.job_orch_policy_id = (
            str(job_orch_policy_id or "").strip().lower()
            or get_default_policy_id_for_binding("service_internal")
        )
        self.taskpool_policy_id = (
            str(taskpool_policy_id or "").strip().lower()
            or get_default_policy_id_for_binding("taskpool_default")
        )
        env_admin_token = str(os.getenv("PYCLOUD_JOB_ORCHESTRATOR_ADMIN_TOKEN", "") or "").strip()
        fallback_admin_token = str(os.getenv("PYCLOUD_INFOCENTER_TOKEN", "") or "").strip()
        self.admin_token = str(admin_token or env_admin_token or fallback_admin_token or "").strip()

        self.service_id = uuid.uuid4().hex
        service_module_name = "pycloud_parallel.controlplane.job_orchestrator_service"
        self._service_module = importlib.import_module(service_module_name)
        mount = self.mount_python_module_service(
            service_name=self.service_name,
            entry_module=service_module_name,
            export_methods=("submit_job", "get_job_status", "cancel_job", "reorder_job"),
            service_id=self.service_id,
            worker_count=1,
            policy_id=self.job_orch_policy_id,
            managed_global_names=(
                "service_id",
                "service_name",
                "queue_capacity",
                "taskpool_policy_id",
                "admin_token",
                "controlplane_target",
                "base_url",
                "render_job_detail_page",
            ),
        )
        self.service_id = mount.service_id
        self.update_globals(self._managed_globals(), service_id=self.service_id)
        self.module = self._service_module.business_module(self.service_id)
        self.job_queue = self.module.job_queue
        self.base_url = ""
        self._stopped = threading.Event()

    def _managed_globals(self) -> Dict[str, object]:
        return {
            "service_id": self.service_id,
            "service_name": self.service_name,
            "queue_capacity": self.queue_capacity,
            "taskpool_policy_id": self.taskpool_policy_id,
            "admin_token": self.admin_token,
            "controlplane_target": self.infocenter_addr,
            "render_job_detail_page": self._render_job_detail_page,
        }

    def start(self) -> None:
        self._stopped.clear()
        logger.info(
            "[JobOrch] start bind=%s infocenter=%s node_id=%s service_name=%s service_id=%s job_orch_policy_id=%s taskpool_policy_id=%s",
            self.bind,
            self.infocenter_addr,
            self.node_id,
            self.service_name,
            self.service_id,
            self.job_orch_policy_id,
            self.taskpool_policy_id,
        )
        self.start_mounted_service_gateway()
        self.base_url = self.service_http_base_url
        values = self._managed_globals()
        if self.base_url:
            values["base_url"] = self.base_url
        self.update_globals(values, service_id=self.service_id)
        self.module = self._service_module.business_module(self.service_id)
        self._service_module.start(
            controlplane_target=self.infocenter_addr,
            base_url=self.base_url,
            service_id=self.service_id,
        )
        self.start_infocenter_registration(
            infocenter_target=self.infocenter_addr,
            control_addr="",
            capacity=1,
            queue_capacity=self.queue_capacity,
            tags=self.tags,
            version=self.version,
            metadata={"component": "job-orchestrator"},
        )

    def stop(self, grace: int = 0) -> None:
        del grace
        logger.info(
            "[JobOrch] stop node_id=%s service_name=%s service_id=%s",
            self.node_id,
            self.service_name,
            self.service_id,
        )
        self._service_module.close(service_id=self.service_id)
        super().close()
        self._stopped.set()

    def wait_for_termination(self) -> None:
        self._stopped.wait()

    def _render_job_detail_page(self, job: Dict[str, object]) -> str:
        pretty = json.dumps(job, ensure_ascii=False, indent=2, default=str)
        rows = [
            ("job_id", job.get("job_id", "")),
            ("status", job.get("status", "")),
            ("submitted_at", job.get("submitted_at", "")),
            ("started_at", job.get("started_at", "")),
            ("finished_at", job.get("finished_at", "")),
            ("client_id", job.get("client_id", "")),
            ("current_error", job.get("error", "")),
            ("final_result", job.get("final_result", "")),
        ]
        table_body = "".join(
            f"<tr><th>{html.escape(str(label))}</th><td>{html.escape(str(value or '-'))}</td></tr>"
            for label, value in rows
        )
        payload_block = self._render_json_block(job.get("payload"))
        checkpoint_block = self._render_json_block(job.get("checkpoint"))
        final_result_block = self._render_json_block(job.get("final_result"))
        results_block = self._render_results_table(job.get("results"))
        return (
            "<!doctype html><html><head><meta charset='utf-8'>"
            "<meta http-equiv='refresh' content='10'>"
            f"<title>Job Detail {html.escape(str(job.get('job_id', '') or ''))}</title>"
            "<style>"
            "body{font-family:Menlo,monospace;margin:20px;line-height:1.45;}"
            "table{border-collapse:collapse;width:100%;margin-bottom:18px;}"
            "th,td{border:1px solid #ccc;padding:8px 10px;font-size:13px;vertical-align:top;"
            "word-break:break-word;overflow-wrap:anywhere;white-space:normal;}"
            "th{background:#f5f5f5;text-align:left;width:180px;}"
            "pre{border:1px solid #ccc;background:#fafafa;padding:12px;font-size:12px;"
            "white-space:pre-wrap;word-break:break-word;overflow-wrap:anywhere;}"
            ".note{color:#555;font-size:12px;margin:6px 0 12px;}"
            ".grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:16px;margin-bottom:18px;}"
            ".card{margin-bottom:18px;}"
            ".card h2{margin:0 0 8px 0;font-size:16px;}"
            ".toolbar{display:flex;gap:12px;align-items:center;margin:8px 0 12px;flex-wrap:wrap;}"
            ".toolbar input{padding:8px 10px;font:inherit;min-width:280px;border:1px solid #bbb;}"
            ".result-row-failed{background:#fff1f0;color:#8a1f11;}"
            ".result-row-cancelled{background:#fff7e6;color:#8d5a00;}"
            "details{max-width:100%;}"
            "summary{cursor:pointer;}"
            "</style></head><body>"
            f"<h1>Job Detail</h1><div class='note'>auto_refresh_sec=10</div>"
            f"<table><tbody>{table_body}</tbody></table>"
            "<div class='grid'>"
            f"<section class='card'><h2>Payload</h2>{payload_block}</section>"
            f"<section class='card'><h2>Checkpoint</h2>{checkpoint_block}</section>"
            "</div>"
            f"<section class='card'><h2>Final Result</h2>{final_result_block}</section>"
            f"<section class='card'><h2>Results</h2>{results_block}</section>"
            "<h2>Raw JSON</h2>"
            f"<pre>{html.escape(pretty)}</pre>"
            "<script>"
            "function filterJobResults(){"
            "var input=document.getElementById('task-filter');"
            "if(!input){return;}"
            "var term=(input.value||'').toLowerCase();"
            "document.querySelectorAll('[data-job-result-row]').forEach(function(row){"
            "var task=(row.getAttribute('data-task-id')||'').toLowerCase();"
            "var status=(row.getAttribute('data-status')||'').toLowerCase();"
            "row.style.display=(!term||task.indexOf(term)>=0||status.indexOf(term)>=0)?'':'none';"
            "});"
            "}"
            "</script>"
            "</body></html>"
        )

    @staticmethod
    def _render_json_block(value: object) -> str:
        if value in (None, "", {}, []):
            return "<div class='note'>-</div>"
        rendered = json.dumps(value, ensure_ascii=False, indent=2, default=str)
        return f"<pre>{html.escape(rendered)}</pre>"

    @staticmethod
    def _render_results_table(results: object) -> str:
        if not isinstance(results, list) or not results:
            return "<div class='note'>no results</div>"
        rows: List[str] = []
        for item in results:
            if not isinstance(item, dict):
                continue
            task_id = str(item.get("task_id", "") or "-")
            status_text = str(item.get("status_text", "") or item.get("status", "-"))
            result_preview = item.get("result")
            error_preview = item.get("error")
            result_text = JobOrchestratorServer._render_expandable_json(result_preview, summary_label="result")
            error_text = JobOrchestratorServer._render_expandable_json(error_preview, summary_label="error")
            row_class = ""
            normalized_status = status_text.upper()
            if "FAILED" in normalized_status:
                row_class = " class='result-row-failed'"
            elif "CANCELLED" in normalized_status:
                row_class = " class='result-row-cancelled'"
            rows.append(
                f"<tr data-job-result-row='1' data-task-id='{html.escape(task_id)}' data-status='{html.escape(status_text)}'{row_class}>"
                f"<td>{html.escape(task_id)}</td>"
                f"<td>{html.escape(status_text)}</td>"
                f"<td>{html.escape(str(item.get('attempt', '') or '-'))}</td>"
                f"<td>{result_text}</td>"
                f"<td>{error_text}</td>"
                "</tr>"
            )
        if not rows:
            return "<div class='note'>no results</div>"
        return (
            "<div class='toolbar'>"
            "<label for='task-filter'>task_id filter</label>"
            "<input id='task-filter' type='search' placeholder='filter by task_id or status' oninput='filterJobResults()'>"
            "</div>"
            "<table><thead><tr>"
            "<th>task_id</th><th>status</th><th>attempt</th><th>result</th><th>error</th>"
            "</tr></thead><tbody>"
            + "".join(rows)
            + "</tbody></table>"
        )

    @staticmethod
    def _render_expandable_json(value: object, *, summary_label: str) -> str:
        if value in (None, "", {}, []):
            return "<span class='note'>-</span>"
        try:
            rendered = json.dumps(value, ensure_ascii=False, indent=2, default=str)
        except Exception:
            rendered = repr(value)
        return (
            "<details>"
            f"<summary>{html.escape(summary_label)}</summary>"
            f"<pre>{html.escape(rendered)}</pre>"
            "</details>"
        )
