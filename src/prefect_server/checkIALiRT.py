from pathlib import Path
from typing import Annotated

from prefect import flow
from prefect.events.schemas.deployment_triggers import DeploymentEventTrigger
from pydantic import Field

from prefect_server.constants import PREFECT_CONSTANTS

ialirt_updated = DeploymentEventTrigger(
    name="Trigger I-ALiRT validation on I-ALiRT update",
    expect={PREFECT_CONSTANTS.EVENT.IALIRT_UPDATED},
    match_related={"prefect.resource.name": PREFECT_CONSTANTS.FLOW_NAMES.POLL_IALIRT},  # type: ignore
    parameters={"files": "{{ event.payload.files }}"},
)


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.CHECK_IALIRT,
    log_prints=True,
)
async def check_ialirt_flow(
    files: Annotated[
        list[Path],
        Field(
            json_schema_extra={
                "title": "Files to check",
                "description": "List of I-ALiRT files to check.",
            }
        ),
    ],
) -> None:
    """
    Check I-ALiRT data store data for anomalies.
    """
    pass


check_ialirt_flow.serve(triggers=[ialirt_updated])
