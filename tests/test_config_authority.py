from __future__ import annotations

from pycloud_parallel.controlplane import config


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
