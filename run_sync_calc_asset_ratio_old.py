from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pycloud_parallel import Service
from pycloud_parallel.artifact import Artifact, ArtifactExports

from calc_asset_ratio.ok import calc_asset_ratio


CONTROLPLANE_TARGET ='local'# "127.0.0.1:50051"
SERVICE_NAME = "calc_asset_ratio"
MANAGED_GLOBAL_NAMES = (
    "bench_mark_yield_df",
    "bench_mark_yield_df_weekly",
    "bench_mark_closeprice_df",
)
SERVICE_SERIALIZATION_MODE = "pickle_stable_v1"


# def _build_service_artifact() -> Artifact:
#     return Artifact.from_paths(
#         ROOT_DIR / "calc_asset_ratio",
#         runtime="py3",
#         entry_module="calc_asset_ratio.calc_asset_ratio",
#         entry_callable="get_fund_asset_ratio",
#         exports=ArtifactExports.single("get_fund_asset_ratio"),
#         deps=ArtifactDeps.node_preinstalled(),
#         managed_global_names=MANAGED_GLOBAL_NAMES,
#     )


def main() -> None:
    service_artifact = Artifact.from_module(
        calc_asset_ratio,
        exports=ArtifactExports.export_all(),
        managed_global_names=MANAGED_GLOBAL_NAMES,
    )

    with Service.deploy(
        target=CONTROLPLANE_TARGET,
        service_name=SERVICE_NAME,
        artifact=service_artifact,
        worker_count=10,
        node_count=2,
        serialization_mode=SERVICE_SERIALIZATION_MODE,
    ) as service:
        service.update_globals(calc_asset_ratio.update_globals())
        print("service:", service.service_name)
        print("nodes:", list(service.sessions.keys()))
        service.join()


if __name__ == "__main__":
    main()
