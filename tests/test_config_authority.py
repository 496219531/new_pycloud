from __future__ import annotations

import ast
from pathlib import Path

from pycloud_parallel.controlplane import config


ROOT = Path(__file__).resolve().parents[1]


def test_stable_config_api_matches_compatibility_constants() -> None:
    authority = config.get_config_limit_authority()
    transport = config.get_transport_bounds()
    object_store = config.get_object_store_bounds()

    assert set(config.STABLE_CONFIG_API_EXPORTS).issubset(set(config.__all__))
    assert set(config.COMPATIBILITY_CONFIG_EXPORTS).issubset(set(config.__all__))
    assert authority.transport_bounds == transport
    assert authority.object_store_bounds == object_store

    assert transport.control_http_max_send_bytes == config.CONTROL_HTTP_MAX_SEND_BYTES
    assert transport.control_http_max_receive_bytes == config.CONTROL_HTTP_MAX_RECEIVE_BYTES
    assert transport.service_http_body_max_bytes == config.SERVICE_HTTP_BODY_MAX_BYTES
    assert transport.gateway_http_body_max_bytes == config.GATEWAY_HTTP_BODY_MAX_BYTES
    assert transport.infocenter_http_body_max_bytes == config.INFOCENTER_HTTP_BODY_MAX_BYTES
    assert transport.node_control_http_body_max_bytes == config.NODE_CONTROL_HTTP_BODY_MAX_BYTES
    assert transport.object_http_body_max_bytes == config.OBJECT_HTTP_BODY_MAX_BYTES

    assert object_store.object_chunk_size_bytes == config.OBJECT_CHUNK_SIZE_BYTES
    assert object_store.file_hash_chunk_size_bytes == config.FILE_HASH_CHUNK_SIZE_BYTES
    assert object_store.gateway_max_upload_file_bytes == config.GATEWAY_MAX_UPLOAD_FILE_BYTES
    assert object_store.gateway_max_upload_total_bytes == config.GATEWAY_MAX_UPLOAD_TOTAL_BYTES

    assert config.get_service_http_body_limit_bytes() == config.SERVICE_HTTP_BODY_MAX_BYTES
    assert config.get_gateway_http_body_limit_bytes() == config.GATEWAY_HTTP_BODY_MAX_BYTES
    assert config.get_infocenter_http_body_limit_bytes() == config.INFOCENTER_HTTP_BODY_MAX_BYTES
    assert config.get_http_object_body_limit_bytes() == config.OBJECT_HTTP_BODY_MAX_BYTES
    assert config.get_gateway_upload_limits() == (
        config.GATEWAY_MAX_UPLOAD_FILE_BYTES,
        config.GATEWAY_MAX_UPLOAD_TOTAL_BYTES,
    )


def test_recommended_config_api_tracks_reload_config(monkeypatch) -> None:
    monkeypatch.setenv("PYCLOUD_SERVICE_HTTP_BODY_MAX_BYTES", "345678")
    monkeypatch.setenv("PYCLOUD_OBJECT_HTTP_BODY_MAX_BYTES", "456789")
    monkeypatch.setenv("PYCLOUD_GATEWAY_MAX_UPLOAD_FILE_BYTES", "123456")
    monkeypatch.setenv("PYCLOUD_GATEWAY_MAX_UPLOAD_TOTAL_BYTES", "234567")

    config.reload_config()
    try:
        transport = config.get_transport_bounds()
        object_store = config.get_object_store_bounds()

        assert config.SERVICE_HTTP_BODY_MAX_BYTES == 345678
        assert config.OBJECT_HTTP_BODY_MAX_BYTES == 456789
        assert transport.service_http_body_max_bytes == 345678
        assert transport.object_http_body_max_bytes == 456789
        assert config.get_service_http_body_limit_bytes() == 345678
        assert config.get_http_object_body_limit_bytes() == 456789

        assert object_store.gateway_max_upload_file_bytes == 123456
        assert object_store.gateway_max_upload_total_bytes == 234567
        assert config.get_gateway_upload_limits() == (123456, 234567)
    finally:
        monkeypatch.delenv("PYCLOUD_SERVICE_HTTP_BODY_MAX_BYTES", raising=False)
        monkeypatch.delenv("PYCLOUD_OBJECT_HTTP_BODY_MAX_BYTES", raising=False)
        monkeypatch.delenv("PYCLOUD_GATEWAY_MAX_UPLOAD_FILE_BYTES", raising=False)
        monkeypatch.delenv("PYCLOUD_GATEWAY_MAX_UPLOAD_TOTAL_BYTES", raising=False)
        config.reload_config()


def test_binding_payload_thresholds_follow_default_safe_policy() -> None:
    soft, hard, result_hard = config.get_binding_payload_thresholds(
        "gateway_public",
        requested_mode="structured_v1",
        context="gateway_public",
    )

    assert soft == config.DEFAULT_SAFE_INLINE_PAYLOAD_SOFT_LIMIT_BYTES
    assert hard == config.DEFAULT_SAFE_INLINE_PAYLOAD_HARD_LIMIT_BYTES
    assert result_hard == config.DEFAULT_SAFE_INLINE_RESULT_HARD_LIMIT_BYTES


def test_core_transport_http_modules_do_not_import_body_limit_constants_directly() -> None:
    scanned_files = [
        ROOT / "src/pycloud_parallel/controlplane/client_transport.py",
        ROOT / "src/pycloud_parallel/controlplane/gateway_http.py",
        ROOT / "src/pycloud_parallel/controlplane/http_gateway.py",
        ROOT / "src/pycloud_parallel/controlplane/infocenter_http.py",
        ROOT / "src/pycloud_parallel/controlplane/node_control_http.py",
        ROOT / "src/pycloud_parallel/controlplane/node_object_http.py",
        ROOT / "src/pycloud_parallel/controlplane/node_capability.py",
    ]
    banned_imports = {
        "SERVICE_HTTP_BODY_MAX_BYTES": "get_service_http_body_limit_bytes(...)",
        "GATEWAY_HTTP_BODY_MAX_BYTES": "get_gateway_http_body_limit_bytes(...)",
        "INFOCENTER_HTTP_BODY_MAX_BYTES": "get_infocenter_http_body_limit_bytes(...)",
        "NODE_CONTROL_HTTP_BODY_MAX_BYTES": "get_node_control_http_body_limit_bytes(...)",
        "OBJECT_HTTP_BODY_MAX_BYTES": "get_http_object_body_limit_bytes(...)",
        "CONTROL_HTTP_MAX_SEND_BYTES": "get_transport_bounds().control_http_max_send_bytes",
        "CONTROL_HTTP_MAX_RECEIVE_BYTES": "get_transport_bounds().control_http_max_receive_bytes",
        "GATEWAY_MAX_UPLOAD_FILE_BYTES": "get_gateway_upload_limits(...)",
        "GATEWAY_MAX_UPLOAD_TOTAL_BYTES": "get_gateway_upload_limits(...)",
    }
    violations: list[str] = []

    for path in scanned_files:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            if node.module != "pycloud_parallel.controlplane.config":
                continue
            for alias in node.names:
                if alias.name in banned_imports:
                    rel = path.relative_to(ROOT)
                    violations.append(f"{rel}:{node.lineno} imports {alias.name}; use {banned_imports[alias.name]}")

    assert not violations, "Core transport/http code must use config authority helpers:\n" + "\n".join(violations)


def test_core_scheduling_paths_do_not_filter_by_node_capability_limits() -> None:
    scanned_files = [
        ROOT / "src/pycloud_parallel/controlplane/infocenter_client.py",
        ROOT / "src/pycloud_parallel/execution/service_session.py",
        ROOT / "src/pycloud_parallel/execution/task_pool.py",
    ]
    banned_fields = {
        "max_control_send_bytes",
        "max_control_recv_bytes",
        "max_http_body_bytes",
        "max_upload_file_bytes",
        "max_upload_total_bytes",
    }
    violations: list[str] = []

    for path in scanned_files:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr in banned_fields:
                rel = path.relative_to(ROOT)
                violations.append(f"{rel}:{node.lineno} reads capability limit field {node.attr}")

    assert not violations, "Task/service candidate paths must not use NodeCapability limit fields:\n" + "\n".join(violations)


def test_service_and_task_candidate_paths_use_shared_node_admission_helper() -> None:
    scanned_files = [
        ROOT / "src/pycloud_parallel/controlplane/infocenter_client.py",
        ROOT / "src/pycloud_parallel/execution/service_session.py",
    ]
    helper_names = {"node_admission_block_reason", "is_admitted_node"}
    banned_phrases = [
        "node.healthy and node.schedulable",
        "node.schedulable and not node.drain",
        "node.healthy and node.schedulable and not node.drain",
    ]
    violations: list[str] = []

    for path in scanned_files:
        text = path.read_text(encoding="utf-8")
        rel = path.relative_to(ROOT)
        if not any(name in text for name in helper_names):
            violations.append(f"{rel} does not reference shared node admission helper")
        for phrase in banned_phrases:
            if phrase in text:
                violations.append(f"{rel} contains hand-written admission phrase: {phrase}")

    assert not violations, "Use scheduling_policy node admission helpers for new task/service candidates:\n" + "\n".join(violations)


def test_node_profile_boundary_does_not_introduce_complex_node_management() -> None:
    scanned_files = [
        ROOT / "src/pycloud_parallel/controlplane/infocenter_state.py",
        ROOT / "src/pycloud_parallel/controlplane/infocenter_client.py",
        ROOT / "src/pycloud_parallel/controlplane/infocenter_http.py",
        ROOT / "src/pycloud_parallel/controlplane/scheduling_policy.py",
    ]
    banned_terms = {
        "NodeManager": "keep node management as minimal endpoint profiles",
        "NodeInventory": "do not add a local inventory authority",
        "machine_id": "endpoint is the first profile identity key",
    }
    violations: list[str] = []

    for path in scanned_files:
        text = path.read_text(encoding="utf-8")
        rel = path.relative_to(ROOT)
        for term, guidance in banned_terms.items():
            if term in text:
                violations.append(f"{rel} contains {term}; {guidance}")

    assert not violations, "Node profile boundary should stay intentionally small:\n" + "\n".join(violations)
