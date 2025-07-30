from pathlib import Path
from typing import Annotated

from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from pydantic import Field

from imap_mag.cli.publish import publish
from imap_mag.util import CONSTANTS, Environment
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.prefectUtils import get_secret_or_env_var


def generate_flow_run_name() -> str:
    parameters = flow_run.parameters
    files: list[Path] = parameters["files"]

    return f"Publish-{','.join([str(f) for f in files])}-to-SDC"


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.PUBLISH,
    log_prints=True,
    flow_run_name=generate_flow_run_name,
)
async def publish_flow(
    files: Annotated[
        list[Path],
        Field(
            json_schema_extra={
                "title": "Files to publish",
                "description": "Paths to the files to publish.",
            }
        ),
    ],
):
    """
    Publish files to the SDC.
    """

    logger = get_run_logger()

    auth_code = await get_secret_or_env_var(
        PREFECT_CONSTANTS.POLL_SCIENCE.SDC_AUTH_CODE_SECRET_NAME,
        CONSTANTS.ENV_VAR_NAMES.SDC_AUTH_CODE,
    )

    logger.info(f"Publishing {len(files)} files: {', '.join(str(f) for f in files)}")

    with Environment(CONSTANTS.ENV_VAR_NAMES.SDC_AUTH_CODE, auth_code):
        publish(files)
