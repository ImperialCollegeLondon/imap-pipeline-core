import asyncio
import os
import sys
from datetime import datetime

import prefect
import prefect.blocks
import prefect.deployments
from prefect import deploy, flow, get_client, serve
from prefect.client.schemas.objects import (
    ConcurrencyLimitConfig,
    ConcurrencyLimitStrategy,
)
from prefect.variables import Variable
from prefect_shell import ShellOperation

from mag_toolkit.calibration.MatlabWrapper import call_matlab


class CONSTANTS:
    DEFAULT_WORKPOOL = "default-pool"
    DEPLOYMENT_TAG = "NASA_IMAP"


@flow(log_prints=True)
def run_matlab():
    print("Starting MATLAB functionality...")
    call_matlab()


@flow(log_prints=True)
def run_imap_pipeline():
    print("Starting IMAP pipeline")

    ShellOperation(
        commands=[
            "./entrypoint.sh",
        ],
        env={"today": datetime.today().strftime("%Y%m%d")},
    ).run()

    print("Finished IMAP pipeline")


async def setupOtherServerConfig():
    # Set a concurrency limit of 10 on the 'autoflow_kernels' tag
    async with get_client() as client:
        # Check if the limit already exists

        try:
            existing_limit = await client.read_global_concurrency_limit_by_name(
                "not-a-name"
            )
        except prefect.exceptions.ObjectNotFound:
            existing_limit = None

        print(f"config: {existing_limit}")


def get_cron_from_env(env_var_name: str, default: str | None = None) -> str | None:
    cron = os.getenv(env_var_name, default)

    if cron is None or cron == "":
        return None
    else:
        cron = cron.strip(" '\"")
        print(f"Using cron schedule: {env_var_name}={cron}")
        return cron


async def get_matlab_license_server():
    return await Variable.get(
        "matlab_license",
        default=os.getenv(CONSTANTS.ENV_VAR_NAMES.MATLAB_LICENSE),  # type: ignore
    )


def deploy_flows(local_debug: bool = False):
    asyncio.get_event_loop().run_until_complete(setupOtherServerConfig())

    imap_flow_name = "imappipeline"

    if local_debug:
        # just run the prefect server locally and deploy all the flows to it without params and schedules

        serve(
            run_imap_pipeline.to_deployment(
                name=imap_flow_name,  # type: ignore
            ),
            run_matlab.to_deployment(name="matlab-test"),  # type: ignore
        )
    else:
        # do a full prefect deployment with containers, work-pools, schedules etc

        # Docker image and tag, e.g. so-pipeline-core:latest. May include registry, e.g. ghcr.io/imperialcollegelondon/so-pipeline-core:latest
        docker_image = os.getenv(
            "IMAP_IMAGE",
            "ghcr.io/imperialcollegelondon/imap-pipeline-core",
        )
        docker_tag = os.getenv(
            "IMAP_IMAGE_TAG",
            "main",
        )

        matlab_license = asyncio.get_event_loop().run_until_complete(
            get_matlab_license_server()
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
            WEBPODA_AUTH_CODE=os.getenv("WEBPODA_AUTH_CODE"),
            SDC_AUTH_CODE=os.getenv("SDC_AUTH_CODE"),
            SQLALCHEMY_URL=os.getenv("SQLALCHEMY_URL"),
            PREFECT_LOGGING_EXTRA_LOGGERS="imap_mag,imap_db,mag_toolkit",
            MLM_LICENSE_FILE=matlab_license,
        )
        shared_job_variables = dict(
            env=shared_job_env_variables,
            image_pull_policy="IfNotPresent",
            networks=docker_networks,
            volumes=docker_volumes,
        )

        print(
            f"Deploying IMAP Pipeline to Prefect with docker {docker_image}:{docker_tag}\n Networks: {docker_networks}\n Volumes: {docker_volumes}"
        )

        imap_pipeline_deployable = run_imap_pipeline.to_deployment(
            name=imap_flow_name,
            cron=get_cron_from_env("IMAP_CRON_HEALTHCHECK"),
            job_variables=shared_job_variables,
            concurrency_limit=ConcurrencyLimitConfig(
                limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
            ),
            tags=[CONSTANTS.DEPLOYMENT_TAG],
        )

        matlab_deployable = run_matlab.to_deployment(
            name="MATLAB",
            job_variables=shared_job_variables,
            concurrency_limit=ConcurrencyLimitConfig(
                limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
            ),
            tags=[CONSTANTS.DEPLOYMENT_TAG],
        )

        deployables = (imap_pipeline_deployable, matlab_deployable)

        deploy_ids = deploy(
            *deployables,  # type: ignore
            work_pool_name=CONSTANTS.DEFAULT_WORKPOOL,
            build=False,
            push=False,
            image=f"{docker_image}:{docker_tag}",
        )  # type: ignore

        if len(deploy_ids) != len(deployables):  # type: ignore
            print(f"Incomplete deployment: {deploy_ids}")
            sys.exit(1)


if __name__ == "__main__":
    local_debug = False
    if len(sys.argv) > 1 and sys.argv[1] == "--local":
        local_debug = True

    deploy_flows(local_debug=local_debug)
