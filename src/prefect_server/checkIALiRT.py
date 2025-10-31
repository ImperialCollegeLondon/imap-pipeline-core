from pathlib import Path
from typing import Annotated

from prefect import flow
from prefect.blocks.notifications import MicrosoftTeamsWebhook
from prefect.client.schemas.objects import State
from prefect.runtime import flow_run
from prefect.states import Completed, Failed
from pydantic import Field

from imap_mag.check import IALiRTAnomaly, SeverityLevel
from imap_mag.cli.check.check_ialirt import check_ialirt
from imap_mag.util import DatetimeProvider
from prefect_server.constants import PREFECT_CONSTANTS


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.CHECK_IALIRT,
    log_prints=True,
)
async def check_ialirt_flow(
    files: Annotated[
        list[Path] | None,
        Field(
            json_schema_extra={
                "title": "Files to check",
                "description": "List of I-ALiRT files to check.",
            }
        ),
    ] = None,
    imap_notification_webhook_name: Annotated[
        str,
        Field(
            default=None,
            json_schema_extra={
                "title": "IMAP Webhook Name",
                "description": "Name of the notification webhook to use for IMAP alerts.",
            },
        ),
    ] = PREFECT_CONSTANTS.IMAP_WEBHOOK_BLOCK_NAME,
) -> State:
    """
    Check I-ALiRT data store data for anomalies.
    """

    # If no files are provided, check data from yesterday to today
    start_date = DatetimeProvider.yesterday() if not files else None
    end_date = DatetimeProvider.today() if not files else None

    anomalies: list[IALiRTAnomaly] = check_ialirt(
        start_date=start_date, end_date=end_date, files=files, error_on_anomaly=False
    )

    if not anomalies:
        return Completed(message="No anomalies detected in I-ALiRT data.")
    else:
        # Report anomalies via Microsoft Teams
        imap_webhook_block = await MicrosoftTeamsWebhook.aload(
            imap_notification_webhook_name
        )

        for anomaly in anomalies:
            message_body: str = anomaly.get_anomaly_description()

            if files:
                message_body += (
                    f"\n\nAffected files: {', '.join([f.as_posix() for f in files])}"
                )
            else:
                message_body += f"\n\nAffected date range: {start_date} to {end_date}"

            message_body += f"\n\n[View the run on mag-pipeline.ph.ic.ac.uk](http://mag-pipeline.ph.ic.ac.uk/runs/flow-run/{flow_run.id})"
            message_subject = f"I-ALiRT {anomaly.severity.name} Anomaly Detected"

            imap_webhook_block.notify_type = (  # type: ignore
                "failure" if anomaly.severity == SeverityLevel.Danger else "warning"
            )

            await imap_webhook_block.notify(  # type: ignore
                body=message_body,
                subject=message_subject,
            )

        return Failed(message="Anomalies detected in I-ALiRT data.")
