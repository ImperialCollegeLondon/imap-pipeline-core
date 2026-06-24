from datetime import datetime, timedelta
from typing import Annotated

from prefect import flow
from prefect.runtime import flow_run
from pydantic import Field

from imap_mag.cli.plot.plot_ialirt import plot_ialirt
from imap_mag.config import SaveMode
from imap_mag.util import DatetimeProvider
from prefect_server.constants import PREFECT_CONSTANTS


def generate_flow_run_name() -> str:
    parameters = flow_run.parameters

    start_date = parameters["start_date"].strftime("%d-%m-%YT%H:%M:%S")
    end_date = parameters["end_date"].strftime("%d-%m-%YT%H:%M:%S")

    return f"Plot-IALiRT-from-{start_date}-to-{end_date}"


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.QUICKLOOK_IALIRT,
    log_prints=True,
    flow_run_name=lambda: generate_flow_run_name(),
)
async def quicklook_ialirt_flow(
    start_date: Annotated[
        datetime | None,
        Field(
            json_schema_extra={
                "title": "Start date",
                "description": "Start date for the download. Default is the last update.",
            }
        ),
    ] = None,
    end_date: Annotated[
        datetime | None,
        Field(
            json_schema_extra={
                "title": "End date",
                "description": "End date for the download. Default is the end of the hour.",
            }
        ),
    ] = None,
    combined_plot: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Combined data into one plot",
                "description": "Whether to create a combined plot of all I-ALiRT data from start to end date.",
            }
        ),
    ] = False,
    force_latest_update: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Force update of latest image",
                "description": "Whether to force the update of the latest quicklook I-ALiRT image",
            }
        ),
    ] = False,
    # Used for automated testing only, to override the default datetime provider with a test one
    datetime_provider: Annotated[
        None | DatetimeProvider,
        Field(exclude=True, frozen=True, json_schema_extra={"title": "(Do not use)"}),
    ] = None,
) -> None:
    """
    Plot I-ALiRT data from data store.
    """

    datetime_provider = (
        DatetimeProvider() if datetime_provider is None else datetime_provider
    )

    if start_date is None:
        start_date = datetime_provider.today() - timedelta(days=2)
    if end_date is None:
        end_date = datetime_provider.end_of_today()

    plot_ialirt(
        start_date=start_date,
        end_date=end_date,
        combined_plot=combined_plot,
        save_mode=SaveMode.LocalAndDatabase,
        force_latest_update=force_latest_update,
        datetime_provider=datetime_provider,
    )
