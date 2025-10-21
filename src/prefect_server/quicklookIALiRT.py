from datetime import datetime, timedelta
from typing import Annotated

from prefect import flow
from prefect.runtime import flow_run
from pydantic import Field

from imap_mag.cli.plot.plot_ialirt import plot_ialirt
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
    flow_run_name=generate_flow_run_name,
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
    ] = DatetimeProvider.today() - timedelta(days=2),
    end_date: Annotated[
        datetime | None,
        Field(
            json_schema_extra={
                "title": "End date",
                "description": "End date for the download. Default is the end of the hour.",
            }
        ),
    ] = DatetimeProvider.end_of_today(),
    combined_plot: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Combined data into one plot",
                "description": "Whether to create a combined plot of all I-ALiRT data from start to end date.",
            }
        ),
    ] = False,
) -> None:
    """
    Plot I-ALiRT data from data store.
    """

    plot_ialirt(
        start_date=start_date,
        end_date=end_date,
        combined_plot=combined_plot,
    )
