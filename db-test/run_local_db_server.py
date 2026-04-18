import os

from pycloud_parallel import Service

workernum = 1
infocenter_target = os.environ.get("PYCLOUD_INFOCENTER_TARGET", "127.0.0.1:50051")
deploy_timeout_sec = float(os.environ.get("PYCLOUD_DEPLOY_TIMEOUT_SEC", "60"))
from db_api import local_db_api

def _deploy_group():
    return Service.deploy(
        infocenter_target=infocenter_target,
        service_name="public_data_source",
        source=local_db_api,
        export_mode="all",
        worker_count=1,
        timeout_sec=deploy_timeout_sec,
        
    )


if __name__ == '__main__':

    try:
        group = _deploy_group()
    except RuntimeError as exc:
        msg = str(exc)
        if "sha256 mismatch: expected=py3" in msg:
            raise RuntimeError(
                "Detected client/node protocol mismatch. "
                "Stop old local services and restart with:\n"
                "  scripts\\start_services.bat restart\n"
                "Then run this script again."
            ) from exc
        raise

    group.join()
