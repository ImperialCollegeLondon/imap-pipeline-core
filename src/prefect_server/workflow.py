import asyncio
import os
import sys
from datetime import datetime

from prefect import deploy, flow, serve
from prefect.client.schemas.objects import (
    ConcurrencyLimitConfig,
    ConcurrencyLimitStrategy,
)
from prefect_shell import ShellOperation

from prefect_server.constants import CONSTANTS
from prefect_server.pollHK import poll_hk_flow
from prefect_server.prefectUtils import get_cron_from_env
from prefect_server.serverConfig import ServerConfig


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


def deploy_flows(local_debug: bool = False):
    asyncio.get_event_loop().run_until_complete(ServerConfig.initialise())

    imap_flow_name = "imappipeline"

    if local_debug:
        # just run the prefect server locally and deploy all the flows to it without params and schedules

        serve(
            run_imap_pipeline.to_deployment(
                name=imap_flow_name,
            ),
            poll_hk_flow.to_deployment(
                name=CONSTANTS.FLOW_NAMES.POLL_HK,
            ),
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
            SQLALCHEMY_URL="postgresql+psycopg://postgres:postgres@db:5432/imap",  # os.getenv("SQLALCHEMY_URL"),
            PREFECT_LOGGING_EXTRA_LOGGERS="imap_mag,imap_db,mag_toolkit",
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
            cron=get_cron_from_env(CONSTANTS.ENV_VAR_NAMES.IMAP_PIPELINE_CRON),
            job_variables=shared_job_variables,
            concurrency_limit=ConcurrencyLimitConfig(
                limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
            ),
            tags=[CONSTANTS.PREFECT_TAG],
        )

        poll_hk_deployable = poll_hk_flow.to_deployment(
            name=CONSTANTS.FLOW_NAMES.POLL_HK,
            cron=get_cron_from_env(CONSTANTS.ENV_VAR_NAMES.POLL_HK_CRON),
            job_variables=shared_job_variables,
            tags=[CONSTANTS.PREFECT_TAG],
        )

        deployables = (imap_pipeline_deployable, poll_hk_deployable)

        deploy_ids = deploy(
            *deployables,
            work_pool_name=CONSTANTS.DEFAULT_WORKPOOL,
            build=False,
            push=False,
            image=f"{docker_image}:{docker_tag}",
        )

        if len(deploy_ids) != len(deployables):
            print(f"Incomplete deployment: {deploy_ids}")
            sys.exit(1)


if __name__ == "__main__":
    local_debug = False
    if len(sys.argv) > 1 and sys.argv[1] == "--local":
        local_debug = True

    deploy_flows(local_debug=local_debug)
