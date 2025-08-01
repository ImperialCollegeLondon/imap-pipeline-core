import asyncio
import os
import sys

from prefect import deploy, serve
from prefect.client.schemas.objects import (
    ConcurrencyLimitConfig,
    ConcurrencyLimitStrategy,
)
from prefect.variables import Variable

from imap_mag.util import CONSTANTS
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.performCalibration import (
    apply_flow,
    calibrate_and_apply_flow,
    calibrate_flow,
)
from prefect_server.pollHK import poll_hk_flow
from prefect_server.pollScience import poll_science_flow
from prefect_server.prefectUtils import get_cron_from_env
from prefect_server.publishFlow import publish_flow
from prefect_server.serverConfig import ServerConfig


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
    matlab_docker_image = os.getenv(
        "IMAP_MATLAB_IMAGE",
        "ghcr.io/imperialcollegelondon/imap-pipeline-core/matlab-imap-pipeline-core",
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

    matlab_license = asyncio.get_event_loop().run_until_complete(
        get_matlab_license_server()
    )

    # remove empty strings
    docker_volumes = [x for x in docker_volumes if x]
    docker_networks = [x for x in docker_networks if x]

    shared_job_env_variables = dict(
        {
            PREFECT_CONSTANTS.ENV_VAR_NAMES.DATA_STORE_OVERRIDE: "/data/",
            CONSTANTS.ENV_VAR_NAMES.SDC_URL: os.getenv(CONSTANTS.ENV_VAR_NAMES.SDC_URL),
            PREFECT_CONSTANTS.ENV_VAR_NAMES.SQLALCHEMY_URL: os.getenv(
                PREFECT_CONSTANTS.ENV_VAR_NAMES.SQLALCHEMY_URL
            ),
            PREFECT_CONSTANTS.ENV_VAR_NAMES.PREFECT_LOGGING_EXTRA_LOGGERS: PREFECT_CONSTANTS.DEFAULT_LOGGERS,
            PREFECT_CONSTANTS.ENV_VAR_NAMES.MATLAB_LICENSE: matlab_license,
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
        name=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.POLL_HK,
        cron=get_cron_from_env(PREFECT_CONSTANTS.ENV_VAR_NAMES.POLL_HK_CRON),
        job_variables=shared_job_variables,
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
    )

    poll_science_norm_l1c_deployable = poll_science_flow.to_deployment(
        name=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.POLL_L1C_NORM,
        parameters={
            "modes": ["norm"],
            "level": "l1c",
        },
        cron=get_cron_from_env(PREFECT_CONSTANTS.ENV_VAR_NAMES.POLL_L1C_NORM_CRON),
        job_variables=shared_job_variables,
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
    )
    poll_science_burst_l1b_deployable = poll_science_flow.to_deployment(
        name=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.POLL_L1B_BURST,
        parameters={
            "modes": ["burst"],
            "level": "l1b",
        },
        cron=get_cron_from_env(PREFECT_CONSTANTS.ENV_VAR_NAMES.POLL_L1B_BURST_CRON),
        job_variables=shared_job_variables,
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
    )
    poll_science_l2_deployable = poll_science_flow.to_deployment(
        name=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.POLL_L2,
        parameters={
            "modes": ["norm", "burst"],
            "level": "l2",
            "reference_frame": "dsrf",
        },
        cron=get_cron_from_env(PREFECT_CONSTANTS.ENV_VAR_NAMES.POLL_L2_CRON),
        job_variables=shared_job_variables,
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
    )

    publish_deployable = publish_flow.to_deployment(
        name=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.PUBLISH,
        job_variables=shared_job_variables,
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
    )

    calibration_deployable = calibrate_flow.to_deployment(
        name="calibrate",
        job_variables=shared_job_variables,
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
        ),
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
    )

    apply_deployable = apply_flow.to_deployment(
        name="apply",
        job_variables=shared_job_variables,
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
        ),
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
    )

    calibrate_and_apply_deployable = calibrate_and_apply_flow.to_deployment(
        name="calibrate_and_apply",
        job_variables=shared_job_variables,
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
        ),
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
    )

    matlab_deployables = (
        calibration_deployable,
        apply_deployable,
        calibrate_and_apply_deployable,
    )

    deployables = (
        poll_hk_deployable,
        poll_science_norm_l1c_deployable,
        poll_science_burst_l1b_deployable,
        poll_science_l2_deployable,
        publish_deployable,
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
            work_pool_name=PREFECT_CONSTANTS.DEFAULT_WORKPOOL,
            build=False,
            push=False,
            image=f"{docker_image}:{docker_tag}",
        )  # type: ignore

        matlab_deploy_ids = deploy(
            *matlab_deployables,  # type: ignore
            work_pool_name=PREFECT_CONSTANTS.DEFAULT_WORKPOOL,
            build=False,
            push=False,
            image=f"{matlab_docker_image}:{matlab_docker_tag}",
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
