from __future__ import annotations

"""中文说明：本地并行配置模型。

仅支持代码内配置，不读取 yaml/环境变量。
"""

import os
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class ProjectConfig:
    """项目配置数据类。"""

    name: str
    cpu_quota: int = 1


@dataclass
class RuntimeConfig:
    """运行时配置数据类（本地模式）。"""

    max_workers: int = 1
    projects: Dict[str, ProjectConfig] = field(default_factory=dict)
    default_project: str = "default"

    @classmethod
    def default(cls) -> "RuntimeConfig":
        cpu = max(1, os.cpu_count() or 1)
        default_project = ProjectConfig(name="default", cpu_quota=cpu)
        return cls(
            max_workers=cpu,
            projects={default_project.name: default_project},
            default_project=default_project.name,
        )


def normalize_runtime_config(config: RuntimeConfig) -> RuntimeConfig:
    """规范化配置，补齐必要默认值。"""
    cfg = RuntimeConfig(
        max_workers=max(1, int(config.max_workers)),
        projects=dict(config.projects),
        default_project=str(config.default_project),
    )
    if not cfg.projects:
        default_project = RuntimeConfig.default().projects["default"]
        cfg.projects = {default_project.name: default_project}
    if cfg.default_project not in cfg.projects:
        cfg.default_project = next(iter(cfg.projects.keys()))
    return cfg


def load_runtime_config() -> RuntimeConfig:
    """返回本地运行时默认配置。"""
    return RuntimeConfig.default()
