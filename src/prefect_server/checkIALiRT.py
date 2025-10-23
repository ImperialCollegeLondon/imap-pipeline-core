from pathlib import Path
from typing import Annotated

from prefect import flow
from prefect.blocks.notifications import MicrosoftTeamsWebhook
from prefect.runtime import flow_run
from prefect.states import Completed, Failed, State
from pydantic import Field

from imap_mag.check import IALiRTAnomaly, SeverityLevel
from imap_mag.cli.check.check_ialirt import check_ialirt
from prefect_server.constants import PREFECT_CONSTANTS


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
    imap_notification_webhook_name: Annotated[
        str,
        Field(
            default=None,
            json_schema_extra={
                "title": "IMAP Webhook Name",
                "description": "Name of the notification webhook to use for IMAP alerts.",
            },
        ),
    ] = PREFECT_CONSTANTS.IMAP_WEBHOOK_NAME,
) -> State:
    """
    Check I-ALiRT data store data for anomalies.
    """

    anomalies: list[IALiRTAnomaly] = check_ialirt(files=files, error_on_anomaly=False)

    if not anomalies:
        return Completed(message="No anomalies detected in I-ALiRT data.")
    else:
        # Report anomalies via Microsoft Teams
        imap_webhook_block = await MicrosoftTeamsWebhook.aload(
            imap_notification_webhook_name
        )

        for anomaly in anomalies:
            message_body: str = anomaly.get_anomaly_description()
            message_body += (
                f"\n\nAffected files: {', '.join([f.as_posix() for f in files])}"
            )
            message_body += f"\n\n[View the run on mag-pipeline.ph.ic.ac.uk](http://mag-pipeline.ph.ic.ac.uk/runs/flow-run/{flow_run.id})"

            if anomaly.severity == SeverityLevel.Danger:
                imap_webhook_block.notify_type = "failure"  # type: ignore
                await imap_webhook_block.notify(
                    body=message_body,
                    subject="I-ALiRT Danger Anomaly Detected",
                )  # type: ignore
            else:
                imap_webhook_block.notify_type = "warning"  # type: ignore
                await imap_webhook_block.notify(
                    body=message_body,
                    subject="I-ALiRT Warning Anomaly Detected",
                )  # type: ignore

        return Failed(message="Anomalies detected in I-ALiRT data.")
