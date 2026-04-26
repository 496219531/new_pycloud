from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pycloud_parallel import Service
from pycloud_parallel.controlplane.artifact import Artifact, ArtifactDeps, ArtifactExports

from calc_asset_ratio.ok import calc_asset_ratio


CONTROLPLANE_TARGET = "127.0.0.1:50051"
SERVICE_NAME = "calc_asset_ratio"
MANAGED_GLOBAL_NAMES = (
    "bench_mark_yield_df",
    "bench_mark_yield_df_weekly",
    "bench_mark_closeprice_df",
)


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
    with Service.deploy(
        infocenter_target=CONTROLPLANE_TARGET,
        service_name=SERVICE_NAME,
        source=calc_asset_ratio,
        export_mode = "all",
        worker_count=7,
        node_count=2,
        managed_global_names=MANAGED_GLOBAL_NAMES,
        policy_id='trusted_internal'
    ) as service:
        # service.update_globals(calc_asset_ratio.update_globals())
        print("service:", service.service_name)
        print("nodes:", list(service.sessions.keys()))
        service.join()


if __name__ == "__main__":
    main()