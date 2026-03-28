from __future__ import annotations

"""中文说明：配置模型与加载逻辑。

优先级约定：默认值 < pycloud.yaml < 环境变量。
用于支撑多集群配置与多项目默认策略。
"""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class ClusterConfig:
    name: str
    address: str = "local"
    weight: float = 1.0
    capacity: int = 1
    healthcheck: str = ""
    use_ray: bool = True


@dataclass
class ProjectConfig:
    name: str
    cpu_quota: int = 1
    mem_quota: int = 0
    priority: int = 1
    default_retries: int = 0
    default_on_error: str = "skip"


@dataclass
class RuntimeConfig:
    clusters: List[ClusterConfig] = field(default_factory=list)
    projects: Dict[str, ProjectConfig] = field(default_factory=dict)
    default_project: str = "default"

    @classmethod
    def default(cls) -> "RuntimeConfig":
        # 默认使用单本地集群 + default 项目，确保开箱可跑。
        cpu = max(1, os.cpu_count() or 1)
        default_cluster = ClusterConfig(name="local", address="local", weight=1.0, capacity=cpu)
        default_project = ProjectConfig(name="default", cpu_quota=cpu, mem_quota=0, priority=1)
        return cls(
            clusters=[default_cluster],
            projects={default_project.name: default_project},
            default_project=default_project.name,
        )


def _parse_cluster(raw: dict, fallback_name: str) -> ClusterConfig:
    return ClusterConfig(
        name=str(raw.get("name", fallback_name)),
        address=str(raw.get("address", "local")),
        weight=float(raw.get("weight", 1.0)),
        capacity=max(1, int(raw.get("capacity", 1))),
        healthcheck=str(raw.get("healthcheck", "")),
        use_ray=bool(raw.get("use_ray", True)),
    )


def _parse_project(name: str, raw: dict) -> ProjectConfig:
    return ProjectConfig(
        name=name,
        cpu_quota=max(1, int(raw.get("cpu_quota", 1))),
        mem_quota=max(0, int(raw.get("mem_quota", 0))),
        priority=max(1, int(raw.get("priority", 1))),
        default_retries=max(0, int(raw.get("default_retries", 0))),
        default_on_error=str(raw.get("default_on_error", "skip")),
    )


def _load_yaml(path: Path) -> dict:
    # YAML 解析做成可选依赖，避免强制安装。
    try:
        import yaml  # type: ignore
    except ImportError:
        raise RuntimeError(
            "PyYAML is required for pycloud.yaml support. "
            "Install with `pip install pycloud-parallel[yaml]`."
        )
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"invalid YAML config in {path}")
    return data


def _merge_env_overrides(cfg: RuntimeConfig) -> RuntimeConfig:
    # 支持用 JSON 字符串快速覆盖集群/项目配置，便于容器化部署。
    clusters_json = os.getenv("PYCLOUD_CLUSTERS", "").strip()
    if clusters_json:
        parsed = json.loads(clusters_json)
        if not isinstance(parsed, list):
            raise ValueError("PYCLOUD_CLUSTERS must be a JSON list")
        cfg.clusters = [_parse_cluster(item, f"cluster-{i}") for i, item in enumerate(parsed)]

    projects_json = os.getenv("PYCLOUD_PROJECTS", "").strip()
    if projects_json:
        parsed = json.loads(projects_json)
        if not isinstance(parsed, dict):
            raise ValueError("PYCLOUD_PROJECTS must be a JSON object")
        cfg.projects = {name: _parse_project(name, raw) for name, raw in parsed.items()}

    default_project = os.getenv("PYCLOUD_DEFAULT_PROJECT", "").strip()
    if default_project:
        cfg.default_project = default_project
    return cfg


def load_runtime_config(path: Optional[str] = None) -> RuntimeConfig:
    # 统一配置入口：先读文件，再应用环境变量覆盖。
    cfg = RuntimeConfig.default()
    config_path = path or os.getenv("PYCLOUD_CONFIG", "pycloud.yaml")
    p = Path(config_path)
    if p.exists():
        raw = _load_yaml(p)
        clusters_raw = raw.get("clusters", [])
        if clusters_raw:
            cfg.clusters = [_parse_cluster(item, f"cluster-{i}") for i, item in enumerate(clusters_raw)]

        projects_raw = raw.get("projects", {})
        if projects_raw:
            cfg.projects = {name: _parse_project(name, project) for name, project in projects_raw.items()}

        if "default_project" in raw:
            cfg.default_project = str(raw["default_project"])

    cfg = _merge_env_overrides(cfg)

    if not cfg.clusters:
        cfg.clusters = RuntimeConfig.default().clusters
    if not cfg.projects:
        default_project = RuntimeConfig.default().projects["default"]
        cfg.projects = {default_project.name: default_project}
    if cfg.default_project not in cfg.projects:
        cfg.default_project = next(iter(cfg.projects.keys()))

    return cfg
