from typing import Annotated

from prefect import flow
from prefect.runtime import flow_run
from pydantic import Field, SecretStr

from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import AutomaticRunParameters, FetchByDatesRunParameters
from imap_mag.data_pipelines.LoPivotPlatformPipeline import LoPivotPlatformPipeline
from imap_mag.db import Database
from imap_mag.util import CONSTANTS, DatetimeProvider
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.prefectUtils import get_secret_or_env_var


class PollLoPivotPlatformFlow:
    def __init__(self, datetime_provider: DatetimeProvider = DatetimeProvider()):
        self._datetime_provider = datetime_provider
        self.poll_lo_pivot_platform_flow = flow(
            self._poll_lo_pivot_platform_flow_impl,
            name=PREFECT_CONSTANTS.FLOW_NAMES.POLL_LO_PIVOT_PLATFORM,
            log_prints=True,
            flow_run_name=self._generate_flow_run_name,
        )

    def _generate_flow_run_name(self) -> str:
        parameters = flow_run.parameters["run_parameters"]

        start_date: str = (
            parameters.start_date.strftime("%d-%m-%Y")
            if hasattr(parameters, "start_date") and parameters.start_date is not None
            else "last-update"
        )
        end_date = (
            parameters.end_date
            if hasattr(parameters, "end_date") and parameters.end_date is not None
            else self._datetime_provider.end_of_today()
        )

        return f"Download-LO-PivotAngle-from-{start_date}-to-{end_date.strftime('%d-%m-%Y')}"

    async def _poll_lo_pivot_platform_flow_impl(
        self,
        run_parameters: Annotated[
            AutomaticRunParameters | FetchByDatesRunParameters,
            Field(
                json_schema_extra={
                    "title": "Run parameters",
                    "description": "Parameters for the pipeline run. If 'start_date' and 'end_date' are not provided, the pipeline will automatically determine the date range based on the last workflow progress.",
                }
            ),
        ] = AutomaticRunParameters(),
        # Avoid reading/writing to db - just download the files
        use_database: bool = True,
    ):
        """Poll low pivot platform angle data from WebTCAD LaTiS API."""

        database = Database() if use_database else None
        settings = AppSettings()

        auth_code = await get_secret_or_env_var(
            PREFECT_CONSTANTS.POLL_LO_PIVOT_PLATFORM.WEBPODA_AUTH_CODE_SECRET_NAME,
            CONSTANTS.ENV_VAR_NAMES.WEBPODA_AUTH_CODE,
        )
        settings.fetch_webtcad.api.auth_code = SecretStr(auth_code)

        pipeline = LoPivotPlatformPipeline(
            database=database,
            settings=settings,
            datetime_provider=self._datetime_provider,
        )
        pipeline.build(run_parameters)
        await pipeline.run()
        result = pipeline.get_results()

        if not result.success:
            raise RuntimeError(f"Pipeline failed: {result}")


_default_flow = PollLoPivotPlatformFlow()
poll_lo_pivot_platform_flow = _default_flow.poll_lo_pivot_platform_flow
generate_flow_run_name = _default_flow._generate_flow_run_name
