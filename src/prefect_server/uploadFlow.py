from pathlib import Path
from typing import Annotated

from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from pydantic import Field

from imap_mag.api.upload import upload
from prefect_server.constants import CONSTANTS
from prefect_server.prefectUtils import get_secret_or_env_var


def generate_flow_run_name() -> str:
    parameters = flow_run.parameters
    files: list[str] = parameters["files"]

    return f"Upload-{','.join([f for f in files])}-to-SDC"


@flow(
    name=CONSTANTS.FLOW_NAMES.UPLOAD,
    log_prints=True,
    flow_run_name=generate_flow_run_name,
)
async def upload_flow(
    files: Annotated[
        list[Path],
        Field(
            json_schema_extra={
                "title": "Files to upload",
                "description": "Paths to the files to upload.",
            }
        ),
    ],
):
    """
    Upload files to the SDC.
    """

    logger = get_run_logger()

    auth_code = await get_secret_or_env_var(
        CONSTANTS.POLL_SCIENCE.SDC_AUTH_CODE_SECRET_NAME,
        CONSTANTS.ENV_VAR_NAMES.SDC_AUTH_CODE,
    )

    logger.info(f"Uploading {len(files)} files:\n{', '.join(str(f) for f in files)}")

    upload(files, auth_code=auth_code)
