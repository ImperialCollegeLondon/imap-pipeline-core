import asyncio
import os
import sys

from prefect import aserve, deploy
from prefect.client.schemas.objects import (
    ConcurrencyLimitConfig,
    ConcurrencyLimitStrategy,
)
from prefect.events import DeploymentEventTrigger
from prefect.schedules import Cron
from prefect.variables import Variable

from imap_mag.util import CONSTANTS
from prefect_server.checkIALiRT import check_ialirt_flow
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.datastoreCleanupFlow import cleanup_datastore_flow
from prefect_server.performCalibration import (
    apply_flow,
    calibrate_and_apply_flow,
    calibrate_flow,
    gradiometry_flow,
)
from prefect_server.pollHK import poll_hk_flow
from prefect_server.pollIALiRT import poll_ialirt_flow
from prefect_server.pollScience import poll_science_flow
from prefect_server.pollSpice import poll_spice_flow
from prefect_server.pollWebTCADLaTiS import poll_webtcad_latis_flow
from prefect_server.postgresUploadFlow import upload_new_files_to_postgres
from prefect_server.prefectUtils import get_cron_from_env
from prefect_server.publishFlow import publish_flow
from prefect_server.quicklookIALiRT import quicklook_ialirt_flow
from prefect_server.serverConfig import ServerConfig
from prefect_server.uploadSharedDocsFlow import upload_shared_docs_flow


async def get_matlab_license_server():
    return await Variable.aget(
        "matlab_license",
        default=os.getenv("MLM_LICENSE_FILE"),  # type: ignore
    )


def deploy_flows(local_debug: bool = False):
    asyncio.run(adeploy_flows(local_debug))


async def adeploy_flows(local_debug: bool = False):
    await ServerConfig.initialise(local_debug)

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

    matlab_license = await get_matlab_license_server()

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

    # Any MAG_SOME_ENV_VAL  env variables will get propagated to the jobs, overriding shared_job_env_variables
    mag_env_vars = {
        key: value for key, value in os.environ.items() if key.startswith("MAG_")
    }

    # Merge dictionaries with mag_env_vars overriding shared_job_env_variables
    shared_job_env_variables = {**shared_job_env_variables, **mag_env_vars}

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

    poll_ialirt_deployable = poll_ialirt_flow.to_deployment(
        name=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.POLL_IALIRT,
        cron=get_cron_from_env(PREFECT_CONSTANTS.ENV_VAR_NAMES.POLL_IALIRT_CRON),
        job_variables=shared_job_variables,
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
        ),
    )

    poll_hk_deployable = poll_hk_flow.to_deployment(
        name=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.POLL_HK,
        cron=get_cron_from_env(PREFECT_CONSTANTS.ENV_VAR_NAMES.POLL_HK_CRON),
        job_variables=shared_job_variables
        | {"mem_limit": "8G", "memswap_limit": "10G"},
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
    )

    poll_spice_deployable = poll_spice_flow.to_deployment(
        name=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.POLL_SPICE,
        cron=get_cron_from_env(PREFECT_CONSTANTS.ENV_VAR_NAMES.POLL_SPICE_CRON),
        job_variables=shared_job_variables,
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
    )

    poll_webtcad_latis_deployable = poll_webtcad_latis_flow.to_deployment(
        name=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.POLL_WEBTCAD_LATIS,
        cron=get_cron_from_env(PREFECT_CONSTANTS.ENV_VAR_NAMES.POLL_WEBTCAD_LATIS_CRON),
        job_variables=shared_job_variables,
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
    )

    sci_polling_schedules = []
    timezone = "Europe/London"
    if get_cron_from_env(PREFECT_CONSTANTS.ENV_VAR_NAMES.POLL_L1C_NORM_CRON):
        sci_polling_schedules.append(
            Cron(
                get_cron_from_env(PREFECT_CONSTANTS.ENV_VAR_NAMES.POLL_L1C_NORM_CRON),
                timezone=timezone,
                parameters={
                    "modes": ["norm"],
                    "level": "l1c",
                },
                slug=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.POLL_L1C_NORM,
            )
        )
    if get_cron_from_env(PREFECT_CONSTANTS.ENV_VAR_NAMES.POLL_L1B_BURST_CRON):
        sci_polling_schedules.append(
            Cron(
                get_cron_from_env(PREFECT_CONSTANTS.ENV_VAR_NAMES.POLL_L1B_BURST_CRON),
                timezone=timezone,
                parameters={
                    "modes": ["burst"],
                    "level": "l1b",
                },
                slug=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.POLL_L1B_BURST,
            )
        )
    if get_cron_from_env(PREFECT_CONSTANTS.ENV_VAR_NAMES.POLL_L2_CRON):
        sci_polling_schedules.append(
            Cron(
                get_cron_from_env(PREFECT_CONSTANTS.ENV_VAR_NAMES.POLL_L2_CRON),
                timezone=timezone,
                parameters={
                    "level": "l2",
                },
                slug=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.POLL_L2,
            )
        )
    if get_cron_from_env(PREFECT_CONSTANTS.ENV_VAR_NAMES.POLL_L1D_CRON):
        sci_polling_schedules.append(
            Cron(
                get_cron_from_env(PREFECT_CONSTANTS.ENV_VAR_NAMES.POLL_L1D_CRON),
                timezone=timezone,
                parameters={"level": "l1d", "modes": ["norm"]},
                slug=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.POLL_L1D + "_norm_only",
            )
        )
        sci_polling_schedules.append(
            Cron(
                get_cron_from_env(PREFECT_CONSTANTS.ENV_VAR_NAMES.POLL_L1D_CRON),
                timezone=timezone,
                parameters={
                    "level": "l1d",
                    "modes": ["burst"],
                    "reference_frames": ["gse", "rtn"],
                },
                slug=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.POLL_L1D
                + "_burst_gse_rtn_only",
            )
        )

    poll_science_deployable = poll_science_flow.to_deployment(
        name=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.POLL_SCIENCE,
        job_variables=shared_job_variables,
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
        schedules=sci_polling_schedules,
    )

    publish_deployable = publish_flow.to_deployment(
        name=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.PUBLISH,
        job_variables=shared_job_variables,
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
    )

    check_ialirt_deployable = check_ialirt_flow.to_deployment(
        name=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.CHECK_IALIRT,
        cron=get_cron_from_env(PREFECT_CONSTANTS.ENV_VAR_NAMES.CHECK_IALIRT_CRON),
        job_variables=shared_job_variables,
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
        ),
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
        triggers=[
            DeploymentEventTrigger(
                name="Trigger I-ALiRT validation on I-ALiRT update",
                expect={PREFECT_CONSTANTS.EVENT.IALIRT_UPDATED},
                match_related={
                    "prefect.resource.name": PREFECT_CONSTANTS.FLOW_NAMES.POLL_IALIRT
                },  # type: ignore
                parameters={
                    "files": {
                        "__prefect_kind": "json",
                        "value": {
                            "__prefect_kind": "jinja",
                            "template": "{{ event.payload.files | tojson }}",
                        },
                    },
                },
            ),
        ],
    )

    quicklook_ialirt_deployable = quicklook_ialirt_flow.to_deployment(
        name=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.QUICKLOOK_IALIRT,
        job_variables=shared_job_variables,
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
    )

    upload_deployable = upload_shared_docs_flow.to_deployment(
        name=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.SHAREPOINT_UPLOAD,
        cron=get_cron_from_env(
            PREFECT_CONSTANTS.ENV_VAR_NAMES.IMAP_CRON_SHAREPOINT_UPLOAD
        ),
        job_variables=shared_job_variables,
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
        ),
        triggers=[
            DeploymentEventTrigger(
                name="Trigger upload after HK poll",
                expect={PREFECT_CONSTANTS.EVENT.FLOW_RUN_COMPLETED},
                match_related={
                    "prefect.resource.name": PREFECT_CONSTANTS.FLOW_NAMES.POLL_HK
                },
            ),
            DeploymentEventTrigger(
                name="Trigger upload after I-ALiRT poll",
                expect={PREFECT_CONSTANTS.EVENT.FLOW_RUN_COMPLETED},
                match_related={
                    "prefect.resource.name": PREFECT_CONSTANTS.FLOW_NAMES.POLL_IALIRT
                },
            ),
            DeploymentEventTrigger(
                name="Trigger upload after science poll",
                expect={PREFECT_CONSTANTS.EVENT.FLOW_RUN_COMPLETED},
                match_related={
                    "prefect.resource.name": PREFECT_CONSTANTS.FLOW_NAMES.POLL_SCIENCE
                },
            ),
            DeploymentEventTrigger(
                name="Trigger upload after APPLY_CALIBRATION",
                expect={PREFECT_CONSTANTS.EVENT.FLOW_RUN_COMPLETED},
                match_related={
                    "prefect.resource.name": PREFECT_CONSTANTS.FLOW_NAMES.APPLY_CALIBRATION
                },
            ),
        ],
    )

    postgres_upload_deployable = upload_new_files_to_postgres.to_deployment(
        name=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.POSTGRES_UPLOAD,
        cron=get_cron_from_env(
            PREFECT_CONSTANTS.ENV_VAR_NAMES.IMAP_CRON_POSTGRES_UPLOAD
        ),
        job_variables=shared_job_variables,
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
        ),
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
        triggers=[
            DeploymentEventTrigger(
                name="Trigger postgres upload after HK poll",
                expect={PREFECT_CONSTANTS.EVENT.FLOW_RUN_COMPLETED},
                match_related={
                    "prefect.resource.name": PREFECT_CONSTANTS.FLOW_NAMES.POLL_HK
                },
            ),
        ],
    )

    datastore_cleanup_deployable = cleanup_datastore_flow.to_deployment(
        name=PREFECT_CONSTANTS.DEPLOYMENT_NAMES.DATASTORE_CLEANUP,
        cron=get_cron_from_env(
            PREFECT_CONSTANTS.ENV_VAR_NAMES.IMAP_CRON_DATASTORE_CLEANUP
        ),
        job_variables=shared_job_variables,
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
        ),
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
    )

    matlab_shared_job_variables = shared_job_variables.copy()
    matlab_shared_job_variables["mem_limit"] = "4g"
    matlab_shared_job_variables["memswap_limit"] = "4g"

    calibration_deployable = calibrate_flow.to_deployment(
        name="calibrate",
        job_variables=matlab_shared_job_variables,
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
        ),
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
    )

    gradiometer_deployable = gradiometry_flow.to_deployment(
        name="gradiometer",
        job_variables=matlab_shared_job_variables,
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
        ),
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
    )

    apply_deployable = apply_flow.to_deployment(
        name="apply",
        job_variables=matlab_shared_job_variables,
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
        ),
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
    )

    calibrate_and_apply_deployable = calibrate_and_apply_flow.to_deployment(
        name="calibrate_and_apply",
        job_variables=matlab_shared_job_variables,
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW
        ),
        tags=[PREFECT_CONSTANTS.PREFECT_TAG],
    )

    matlab_deployables = await asyncio.gather(
        calibration_deployable,
        gradiometer_deployable,
        apply_deployable,
        calibrate_and_apply_deployable,
    )

    deployables = await asyncio.gather(
        poll_ialirt_deployable,
        poll_hk_deployable,
        poll_spice_deployable,
        poll_science_deployable,
        poll_webtcad_latis_deployable,
        publish_deployable,
        check_ialirt_deployable,
        quicklook_ialirt_deployable,
        upload_deployable,
        postgres_upload_deployable,
        datastore_cleanup_deployable,
    )

    if local_debug:
        for deployable in deployables:
            deployable.work_queue_name = None
            deployable.job_variables = {}

        await aserve(
            *deployables,
        )

    else:
        deploy_ids = await deploy(
            *deployables,  # type: ignore
            work_pool_name=PREFECT_CONSTANTS.DEFAULT_WORKPOOL,
            build=False,
            push=False,
            image=f"{docker_image}:{docker_tag}",
        )  # type: ignore

        matlab_deploy_ids = await deploy(
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
