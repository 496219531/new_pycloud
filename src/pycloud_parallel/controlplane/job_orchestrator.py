from __future__ import annotations

import html
import json
import threading
import uuid
from typing import Dict, List, Optional, Tuple

from pycloud_parallel.controlplane.http_gateway import ServiceHttpGateway
from pycloud_parallel.controlplane.job_queue import JobQueueManager
from pycloud_parallel.controlplane.registrar import JobOrchestratorInfoCenterRegistrar
from pycloud_parallel.grpc.v1 import pycloud_v1_pb2 as pb2


DEFAULT_JOB_ORCHESTRATOR_SERVICE_NAME = "job-orchestrator"


class JobOrchestratorServer:
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
    ) -> None:
        self.bind = str(bind or "").strip()
        self.infocenter_addr = str(infocenter_addr or "").strip()
        self.node_id = str(node_id or "job-orchestrator-01").strip() or "job-orchestrator-01"
        self.service_name = str(service_name or DEFAULT_JOB_ORCHESTRATOR_SERVICE_NAME).strip() or DEFAULT_JOB_ORCHESTRATOR_SERVICE_NAME
        self.queue_capacity = max(1, int(queue_capacity or 1))
        self.tags = list(tags or ["job"])
        self.version = str(version or "")

        self.service_id = uuid.uuid4().hex
        self.job_queue = JobQueueManager()
        self._http = ServiceHttpGateway(
            bind=self.bind,
            invoke_handler=self._invoke,
            status_handler=self._status,
            methods_handler=self._methods,
            extra_get_handler=self._extra_get,
        )
        self._registrar = JobOrchestratorInfoCenterRegistrar(
            infocenter_addr=self.infocenter_addr,
            node_id=self.node_id,
            service_id=self.service_id,
            service_name=self.service_name,
            http_base_url_provider=lambda: self.base_url,
            status_provider=self.job_queue.summary,
            queue_capacity=self.queue_capacity,
            tags=self.tags,
            version=self.version,
        )
        self.base_url = ""
        self._stopped = threading.Event()

    def start(self) -> None:
        self._stopped.clear()
        self._http.start()
        self.base_url = self._http.base_url
        self.job_queue.start(controlplane_target=self.infocenter_addr)
        self._registrar.start()

    def stop(self, grace: int = 0) -> None:
        del grace
        self._registrar.close()
        self.job_queue.close()
        self._http.stop()
        self._stopped.set()

    def wait_for_termination(self) -> None:
        self._stopped.wait()

    def _ensure_service(self, service_id: str) -> Optional[Tuple[int, Dict[str, object]]]:
        normalized = str(service_id or "").strip()
        if normalized != self.service_id:
            return 404, {"ok": False, "error": "service not found"}
        return None

    def _invoke(
        self,
        service_id: str,
        method: str,
        payload: Dict[str, object],
        token: str,
        timeout_sec: float,
        serialization_mode: str = "",
    ) -> Tuple[int, Dict[str, object]]:
        del timeout_sec, serialization_mode
        rejected = self._ensure_service(service_id)
        if rejected is not None:
            return rejected
        requested_method = str(method or "").strip()
        if requested_method == "submit_job":
            state = self.job_queue.submit_job(dict(payload or {}), auth_token=token)
            return 200, {"ok": True, "job": state.as_dict()}
        if requested_method in {"get_job", "get_job_status"}:
            job_id = str((payload or {}).get("job_id", "") or "").strip()
            if not job_id:
                return 400, {"ok": False, "error": "job_id is required"}
            state = self.job_queue.get_job(job_id)
            if state is None:
                return 404, {"ok": False, "error": "job not found"}
            return 200, {"ok": True, "job": state.as_dict()}
        if requested_method == "reorder_job":
            job_id = str((payload or {}).get("job_id", "") or "").strip()
            direction = str((payload or {}).get("direction", "") or "").strip().lower()
            if not job_id:
                return 400, {"ok": False, "error": "job_id is required"}
            try:
                state = self.job_queue.reorder_job(job_id, direction=direction)
            except ValueError as exc:
                return 400, {"ok": False, "error": str(exc)}
            if state is None:
                return 404, {"ok": False, "error": "job not found"}
            if state.status != "WAITING":
                return 409, {"ok": False, "error": "only waiting jobs can be reordered", "job": state.as_dict()}
            return 200, {"ok": True, "job": state.as_dict(), "queue": self.job_queue.summary()}
        if requested_method == "cancel_job":
            job_id = str((payload or {}).get("job_id", "") or "").strip()
            if not job_id:
                return 400, {"ok": False, "error": "job_id is required"}
            try:
                state = self.job_queue.cancel_job(job_id, auth_token=token)
            except PermissionError as exc:
                return 403, {"ok": False, "error": str(exc)}
            if state is None:
                return 404, {"ok": False, "error": "job not found"}
            return 200, {"ok": True, "job": state.as_dict()}
        return 404, {"ok": False, "error": f"unknown method: {requested_method}"}

    def _status(self, service_id: str) -> Tuple[int, Dict[str, object]]:
        rejected = self._ensure_service(service_id)
        if rejected is not None:
            return rejected
        queue = self.job_queue.summary()
        return 200, {
            "ok": True,
            "service": {
                "service_id": self.service_id,
                "service_name": self.service_name,
                "status": int(pb2.SERVICE_STATUS_RUNNING),
                "status_text": pb2.ServiceStatus.Name(pb2.SERVICE_STATUS_RUNNING),
                "http_base_url": f"{self.base_url}/svc/{self.service_id}" if self.base_url else "",
                "methods": [item["method"] for item in self._method_specs()],
            },
            "queue": queue,
        }

    def _methods(self, service_id: str, include_docs: bool) -> Tuple[int, Dict[str, object]]:
        rejected = self._ensure_service(service_id)
        if rejected is not None:
            return rejected
        methods = self._method_specs()
        if not include_docs:
            methods = [{**item, "doc": ""} for item in methods]
        return 200, {"ok": True, "service_id": self.service_id, "methods": methods}

    def _extra_get(
        self,
        service_id: str,
        path_parts: List[str],
        query: Dict[str, List[str]],
    ) -> Optional[Tuple[int, Dict[str, object]]]:
        rejected = self._ensure_service(service_id)
        if rejected is not None:
            return rejected
        if len(path_parts) == 2 and path_parts[0] == "jobs":
            job_id = str(path_parts[1] or "").strip()
            if not job_id:
                return 400, {"ok": False, "error": "job_id is required"}
            state = self.job_queue.get_job(job_id)
            if state is None:
                return 404, {"ok": False, "error": "job not found"}
            view = str((query.get("view", [""]) or [""])[0] or "").strip().lower()
            if view == "html":
                return 200, self._render_job_detail_page(state.as_dict()), "text/html; charset=utf-8"
            return 200, {"ok": True, "job": state.as_dict()}
        return None

    @staticmethod
    def _method_specs() -> List[Dict[str, str]]:
        return [
            {
                "method": "submit_job",
                "qualified_name": "job_orchestrator.submit_job",
                "doc": "Submit a job payload to the single job orchestrator.",
            },
            {
                "method": "get_job_status",
                "qualified_name": "job_orchestrator.get_job_status",
                "doc": "Fetch the current state for one job_id.",
            },
            {
                "method": "cancel_job",
                "qualified_name": "job_orchestrator.cancel_job",
                "doc": "Request cancellation for one job_id.",
            },
            {
                "method": "reorder_job",
                "qualified_name": "job_orchestrator.reorder_job",
                "doc": "Move one waiting job up or down inside the queue.",
            },
        ]

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
