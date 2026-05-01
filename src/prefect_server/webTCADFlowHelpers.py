"""Shared helpers for Prefect flows that download telemetry items from WebTCAD LaTiS."""

from prefect.runtime import flow_run
from pydantic import SecretStr

from imap_mag.client.WebTCADLaTiS import HKWebTCADItems
from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import AutomaticRunParameters, FetchByDatesRunParameters
from imap_mag.data_pipelines.WebTCADTelemetryItemPipeline import (
    WebTCADTelemetryItemPipeline,
)
from imap_mag.db import Database
from imap_mag.util import CONSTANTS, DatetimeProvider
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.prefectUtils import get_secret_or_env_var


def make_flow_run_name(label: str):
    """Build a Prefect ``flow_run_name`` callable that produces a name for the
    current run using the supplied human-readable telemetry-item label.

    The shape is ``Download-<label>-from-<start>-to-<end>`` where the dates come
    from the ``run_parameters`` argument of the running flow.
    """

    def _generate_flow_run_name() -> str:
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

        return f"Download-{label}-from-{start_date}-to-{end_date.strftime('%d-%m-%Y')}"

    return _generate_flow_run_name


async def run_webtcad_pipeline(
    item: HKWebTCADItems,
    run_parameters: AutomaticRunParameters | FetchByDatesRunParameters,
    use_database: bool,
) -> None:
    """Build and run a ``WebTCADTelemetryItemPipeline`` for the given telemetry item.

    Loads the WebTCAD LaTiS auth code from the configured Prefect secret block (or the
    environment variable fallback) before kicking off the pipeline. Raises
    ``RuntimeError`` if the pipeline reports failure.
    """

    database = Database() if use_database else None
    settings = AppSettings()

    auth_code = await get_secret_or_env_var(
        PREFECT_CONSTANTS.POLL_WEBTCAD.WEBTCAD_AUTH_CODE_SECRET_NAME,
        CONSTANTS.ENV_VAR_NAMES.WEBPODA_AUTH_CODE,
    )
    settings.fetch_webtcad.api.auth_code = SecretStr(auth_code)

    pipeline = WebTCADTelemetryItemPipeline(
        item=item, database=database, settings=settings
    )
    pipeline.build(run_parameters)
    await pipeline.run()
    result = pipeline.get_results()

    if not result.success:
        raise RuntimeError(f"Pipeline failed: {result}")
