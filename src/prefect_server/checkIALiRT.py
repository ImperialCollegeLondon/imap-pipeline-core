from pathlib import Path
from typing import Annotated

from prefect import flow
from prefect.blocks.notifications import MicrosoftTeamsWebhook
from prefect.events.schemas.deployment_triggers import DeploymentEventTrigger
from pydantic import Field

from imap_mag.check import IALiRTAnomaly
from imap_mag.cli.check.check_ialirt import check_ialirt
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
    notification_webhook_name: Annotated[
        str,
        Field(
            default=None,
            json_schema_extra={
                "title": "Notification Webhook Name",
                "description": "Name of the notification webhook to use for alerts.",
            },
        ),
    ] = PREFECT_CONSTANTS.CHECK_IALIRT.WEBHOOK_NAME,
) -> None:
    """
    Check I-ALiRT data store data for anomalies.
    """

    anomalies: list[IALiRTAnomaly] = check_ialirt(files=files, error_on_failure=False)

    teams_webhook_block = await MicrosoftTeamsWebhook.aload(notification_webhook_name)

    for anomaly in anomalies:
        await teams_webhook_block.notify(
            body=anomaly.get_anomaly_description(), subject="I-ALiRT Anomaly Detected"
        )


check_ialirt_flow.serve(triggers=[ialirt_updated])
