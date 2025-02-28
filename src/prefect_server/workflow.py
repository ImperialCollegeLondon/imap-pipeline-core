import asyncio
import os
import sys
from datetime import date, datetime
from pathlib import Path

from prefect import deploy, flow, serve, settings
from prefect.client.schemas.objects import (
    ConcurrencyLimitConfig,
    ConcurrencyLimitStrategy,
)
from prefect.logging import get_run_logger
from prefect.variables import Variable
from prefect_shell import ShellOperation

from mag_toolkit.calibration.MatlabWrapper import call_matlab


class CONSTANTS:
    DEFAULT_WORKPOOL = "default-pool"
    DEPLOYMENT_TAG = "NASA_IMAP"


@flow(log_prints=True)
def run_matlab():
    logger = get_run_logger()

    logger.info(settings.PREFECT_LOGGING_EXTRA_LOGGERS.value())
    logger.info("Starting MATLAB functionality...")
    call_matlab("helloworld")


@flow(log_prints=True)
def generate_offsets(filename: Path, output_filename: Path):
    logger = get_run_logger()

    logger.info(f"Generating an offsets file based on {filename}")

    if not os.path.isfile(filename):
        logger.error(f"Cannot generate offsets: {filename} is not a file")
        raise ValueError(f"{filename} is not a file")

    if not os.path.isdir(output_filename.parent):
        logger.error(f"Parent directories for {output_filename} do not exist")
        raise ValueError(f"Parent directories for {output_filename} do not exist")

    if os.path.isfile(output_filename):
        logger.error(f"Output file {output_filename} already exists")
        raise ValueError(f"File {output_filename} already exists")

    call_matlab(
        [f'generate_offsets("{filename}", "{output_filename}")'], flow_logger=logger
    )


@flow(log_prints=True)
def run_imap_pipeline(start_date: date, end_date: date):
    print(f"Starting IMAP pipeline for {start_date} to {end_date}")

    ShellOperation(
        commands=[
            f"./entrypoint.sh {start_date.strftime('%Y-%m-%d')} {end_date.strftime('%Y-%m-%d')}"
        ],
        env={"today": datetime.today().strftime("%Y%m%d")},
    ).run()


def get_cron_from_env(env_var_name: str, default: str | None = None) -> str | None:
    cron = os.getenv(env_var_name, default)

    if cron is None or cron == "":
        return None
    else:
        cron = cron.strip(" '\"")
        print(f"Using cron schedule: {env_var_name}={cron}")
        return cron


# todo: this shpould happen at job runtime and not deployment time
async def get_matlab_license_server():
    return await Variable.get(
        "matlab_license",
        default=os.getenv("MLM_LICENSE_FILE"),  # type: ignore
    )


def deploy_flows(local_debug: bool = False):
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

    imap_flow_name = "imappipeline"
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

    offsets_deployable = generate_offsets.to_deployment(
        name="generate-offsets",
        job_variables=shared_job_variables,
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
        ),
        tags=[CONSTANTS.DEPLOYMENT_TAG],
    )

    deployables = (imap_pipeline_deployable, matlab_deployable, offsets_deployable)

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

        if len(deploy_ids) != len(deployables):  # type: ignore
            print(f"Incomplete deployment: {deploy_ids}")
            sys.exit(1)


if __name__ == "__main__":
    local_debug = False
    if len(sys.argv) > 1 and sys.argv[1] == "--local":
        local_debug = True

    deploy_flows(local_debug=local_debug)
