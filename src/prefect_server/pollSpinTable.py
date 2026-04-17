from typing import Annotated

from prefect import flow
from prefect.runtime import flow_run
from pydantic import Field, SecretStr

from imap_mag.client.SDCDataAccess import SDCDataAccess
from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import AutomaticRunParameters, FetchByDatesRunParameters
from imap_mag.data_pipelines.SpinTablePipeline import SpinTablePipeline
from imap_mag.db import Database
from imap_mag.util import CONSTANTS, DatetimeProvider
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.prefectUtils import get_secret_or_env_var


def generate_flow_run_name() -> str:
    parameters = flow_run.parameters["run_parameters"]

    start_date: str = (
        parameters.start_date.strftime("%d-%m-%Y")
        if hasattr(parameters, "start_date") and parameters.start_date is not None
        else "last-update"
    )
    end_date = (
        parameters.end_date
        if hasattr(parameters, "end_date") and parameters.end_date is not None
        else DatetimeProvider.end_of_today()
    )

    return f"Download-SpinTable-from-{start_date}-to-{end_date.strftime('%d-%m-%Y')}"


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.POLL_SPIN_TABLE,
    log_prints=True,
    flow_run_name=generate_flow_run_name,
)
async def poll_spin_table_flow(
    run_parameters: Annotated[
        AutomaticRunParameters | FetchByDatesRunParameters,
        Field(
            json_schema_extra={
                "title": "Run parameters",
                "description": "Parameters for the pipeline run. If 'start_date' and 'end_date' are not provided, the pipeline will automatically determine the date range based on the last workflow progress.",
            }
        ),
    ],
    use_database: bool = True,
):
    """Poll spin table files from SDC API."""

    database = Database() if use_database else None
    settings = AppSettings()

    auth_code = await get_secret_or_env_var(
        PREFECT_CONSTANTS.POLL_SPIN_TABLE.SDC_AUTH_CODE_SECRET_NAME,
        CONSTANTS.ENV_VAR_NAMES.SDC_AUTH_CODE,
    )

    client = SDCDataAccess(
        auth_code=SecretStr(auth_code),
        data_dir=settings.setup_work_folder_for_command(settings.fetch_spice),
        sdc_url=settings.fetch_spice.api.url_base,
    )

    pipeline = SpinTablePipeline(database=database, settings=settings, client=client)
    pipeline.build(run_parameters)
    await pipeline.run()
    result = pipeline.get_results()

    if not result.success:
        raise RuntimeError(f"Pipeline failed: {result}")
