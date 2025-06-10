import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

from prefect import deploy, flow, serve
from prefect.client.schemas.objects import (
    ConcurrencyLimitConfig,
    ConcurrencyLimitStrategy,
)
from prefect.variables import Variable

from imap_mag.api.apply import FileType, apply
from imap_mag.api.calibrate import calibrate
from mag_toolkit.calibration import CalibrationMethod
from prefect_server.constants import CONSTANTS
from prefect_server.pollHK import poll_hk_flow
from prefect_server.pollScience import poll_science_flow
from prefect_server.prefectUtils import get_cron_from_env
from prefect_server.serverConfig import ServerConfig


@flow(name="calibrate", log_prints=True)
def calibrate_flow(
    from_date: datetime, to_date: datetime, method: CalibrationMethod, science_file: str
):
    calibrate(from_date, to_date, method, science_file)


@flow(name="apply calibration", log_prints=True)
def apply_flow(
    cal_layer: Path,
    file: Path,
    date: datetime,
    calibration_output_type: FileType = FileType.CDF,
    L2_output_type: FileType = FileType.CDF,
):
    apply(
        [str(cal_layer)],
        from_date=date,
        to_date=date,
        input=str(file),
        calibration_output_type=calibration_output_type.value,
        l2_output_type=L2_output_type.value,
    )


async def get_matlab_license_server():
    return await Variable.get(
        "matlab_license",
        default=os.getenv("MLM_LICENSE_FILE"),  # type: ignore
    )


def deploy_flows(local_debug: bool = False):
    asyncio.get_event_loop().run_until_complete(ServerConfig.initialise(local_debug))

    # Docker image and tag, e.g. so-pipeline-core:latest. May include registry, e.g. ghcr.io/imperialcollegelondon/so-pipeline-core:latest
    docker_image = os.getenv(
        "IMAP_IMAGE",
        "ghcr.io/imperialcollegelondon/imap-pipeline-core",
    )
    docker_tag = os.getenv(
        "IMAP_IMAGE_TAG",
        "main",
    )

    matlab_docker_tag = os.getenv(
        "IMAP_MATLAB_IMAGE_TAG",
        "matlab-main",
    )
    # Comma separated docker volumes, e.g. /mnt/imap-data/dev:/data
    docker_volumes = os.getenv("IMAP_VOLUMES", "").split(",")
    # Comma separated docker networks, e.g. mag-lab-data-platform,some-other-network
    docker_networks = os.getenv(
        "DOCKER_NETWORK",
        "mag-lab-data-platform",
    ).split(",")

    # remove empty strings
    docker_volumes = [x for x in docker_volumes if x]
    docker_networks = [x for x in docker_networks if x]

    shared_job_env_variables = dict(
        {
            CONSTANTS.ENV_VAR_NAMES.DATA_STORE_OVERRIDE: "/data/",
            CONSTANTS.ENV_VAR_NAMES.WEBPODA_AUTH_CODE: os.getenv(
                CONSTANTS.ENV_VAR_NAMES.WEBPODA_AUTH_CODE
            ),
            CONSTANTS.ENV_VAR_NAMES.SDC_AUTH_CODE: os.getenv(
                CONSTANTS.ENV_VAR_NAMES.SDC_AUTH_CODE
            ),
            CONSTANTS.ENV_VAR_NAMES.SQLALCHEMY_URL: os.getenv(
                CONSTANTS.ENV_VAR_NAMES.SQLALCHEMY_URL
            ),
            CONSTANTS.ENV_VAR_NAMES.PREFECT_LOGGING_EXTRA_LOGGERS: CONSTANTS.DEFAULT_LOGGERS,
        }
    )

    if local_debug:
        shared_job_variables = dict(env=shared_job_env_variables)
        print("Deploying IMAP Pipeline to Prefect with local server")
    else:
        shared_job_variables = dict(
            env=shared_job_env_variables,
            image_pull_policy="IfNotPresent",
            networks=docker_networks,
            volumes=docker_volumes,
        )
        print(
            f"Deploying IMAP Pipeline to Prefect with docker {docker_image}:{docker_tag}\n Networks: {docker_networks}\n Volumes: {docker_volumes}"
        )

    poll_hk_deployable = poll_hk_flow.to_deployment(
        name=CONSTANTS.DEPLOYMENT_NAMES.POLL_HK,
        cron=get_cron_from_env(CONSTANTS.ENV_VAR_NAMES.POLL_HK_CRON),
        job_variables=shared_job_variables,
        tags=[CONSTANTS.PREFECT_TAG],
    )

    poll_science_norm_l1c_deployable = poll_science_flow.to_deployment(
        name=CONSTANTS.DEPLOYMENT_NAMES.POLL_L1C_NORM,
        parameters={
            "modes": ["norm"],
            "level": "l1c",
        },
        cron=get_cron_from_env(CONSTANTS.ENV_VAR_NAMES.POLL_L1C_NORM_CRON),
        job_variables=shared_job_variables,
        tags=[CONSTANTS.PREFECT_TAG],
    )
    poll_science_burst_l1b_deployable = poll_science_flow.to_deployment(
        name=CONSTANTS.DEPLOYMENT_NAMES.POLL_L1B_BURST,
        parameters={
            "modes": ["burst"],
            "level": "l1b",
        },
        cron=get_cron_from_env(CONSTANTS.ENV_VAR_NAMES.POLL_L1B_BURST_CRON),
        job_variables=shared_job_variables,
        tags=[CONSTANTS.PREFECT_TAG],
    )

    calibration_deployable = calibrate_flow.to_deployment(
        name="calibrate",
        job_variables=shared_job_variables,
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
        ),
        tags=[CONSTANTS.PREFECT_TAG],
    )

    apply_deployable = apply_flow.to_deployment(
        name="apply",
        job_variables=shared_job_variables,
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
        ),
        tags=[CONSTANTS.PREFECT_TAG],
    )

    matlab_deployables = (calibration_deployable, apply_deployable)

    deployables = (
        poll_hk_deployable,
        poll_science_norm_l1c_deployable,
        poll_science_burst_l1b_deployable,
    )

    if local_debug:
        for deployable in deployables:
            deployable.work_queue_name = None
            deployable.job_variables = None

        serve(
            *deployables,
        )

    else:
        deploy_ids = deploy(
            *deployables,  # type: ignore
            work_pool_name=CONSTANTS.DEFAULT_WORKPOOL,
            build=False,
            push=False,
            image=f"{docker_image}:{docker_tag}",
        )  # type: ignore

        matlab_deploy_ids = deploy(
            *matlab_deployables,  # type: ignore
            work_pool_name=CONSTANTS.DEFAULT_WORKPOOL,
            build=False,
            push=False,
            image=f"{docker_image}:{matlab_docker_tag}",
        )  # type: ignore

        if len(deploy_ids) != len(deployables) or len(matlab_deploy_ids) != len(  # type: ignore
            matlab_deployables
        ):
            print(f"Incomplete deployment: {deploy_ids} {matlab_deploy_ids}")
            sys.exit(1)


if __name__ == "__main__":
    local_debug = False
    if len(sys.argv) > 1 and sys.argv[1] == "--local":
        local_debug = True

    deploy_flows(local_debug=local_debug)
