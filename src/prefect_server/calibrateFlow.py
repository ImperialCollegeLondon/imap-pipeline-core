from datetime import date, datetime
from pathlib import Path
from typing import Annotated

from prefect import flow
from prefect.client.schemas.objects import FlowRun
from prefect.runtime import flow_run
from pydantic import Field

from imap_mag.cli.calibrate import Sensor
from imap_mag.config import SaveMode
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import CalibrationMethod, LayerDataFormat
from mag_toolkit.calibration.CalibrationConfig import (
    GradiometryConfig,
    SetQualityAndNaNConfig,
)
from prefect_server.calibrationFlowCommon import (
    DEFAULT_CALIBRATION_MATRIX_VERSION,
    DEFAULT_INPUT_JSON_FILE,
    DEFAULT_MATLAB_REPO_PATH,
    PrefectScriptedL2CalibrationConfig,
    SplitByDay,
    _configuration_for_deployment,
    _days_in_range,
    _run_calibration,
    _submit_days_as_deployment_runs,
    logger,
)
from prefect_server.constants import PREFECT_CONSTANTS


def generate_calibration_flow_run_name() -> str:

    parameters = flow_run.parameters
    method_name = parameters["configuration"].get_method()
    start_date: datetime = parameters["start_date"]
    end_date = parameters.get("end_date")
    method: CalibrationMethod = method_name
    mode: ScienceMode = parameters["mode"]
    sensor: Sensor = parameters.get("sensor", Sensor.MAGO)

    date_str = start_date.strftime("%d-%m-%Y")
    if end_date and end_date != start_date:
        date_str = (
            f"{start_date.strftime('%d-%m-%Y')}-to-{end_date.strftime('%d-%m-%Y')}"
        )

    return f"Calibrating-{date_str}-for-{sensor.value}-{mode.value}-with-{method.value}"


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE,
    log_prints=True,
    flow_run_name=generate_calibration_flow_run_name,
)
def calibrate_flow(
    start_date: Annotated[
        date,
        Field(
            json_schema_extra={
                "title": "Start date / single day",
                "description": "Starting content date of file(s) to be calibrated. If end date is not specified, only this day will be calibrated.",
                "position": 1,
            }
        ),
    ],
    end_date: Annotated[
        date | None,
        Field(
            json_schema_extra={
                "title": "End date",
                "description": "Ending content date of file(s) to be calibrated. If not specified, only the start date will be calibrated.",
                "position": 2,
            }
        ),
    ] = None,
    configuration: Annotated[
        PrefectScriptedL2CalibrationConfig | SetQualityAndNaNConfig | GradiometryConfig,
        Field(
            json_schema_extra={
                "title": "Calibration Type Configuration",
                "description": "Configuration to be used for the seslected calibration method.",
                "position": 3,
            }
        ),
    ] = PrefectScriptedL2CalibrationConfig(
        calibration_matrix_version=DEFAULT_CALIBRATION_MATRIX_VERSION,
        input_json_file=DEFAULT_INPUT_JSON_FILE,
        matlab_repo=DEFAULT_MATLAB_REPO_PATH,
    ),
    mode: ScienceMode = ScienceMode.Normal,
    sensor: Sensor = Sensor.MAGO,
    save_mode: SaveMode = SaveMode.LocalAndDatabase,
    metakernel: Path | None = None,
    split_by_day: SplitByDay = False,
    layer_data_format: LayerDataFormat = LayerDataFormat.PARQUET,
) -> list[Path] | list[FlowRun]:

    if end_date and end_date < start_date:
        raise ValueError(
            f"End date {end_date} cannot be before start date {start_date}."
        )

    days = _days_in_range(start_date, end_date)
    if split_by_day and len(days) > 1:
        return _submit_days_as_deployment_runs(
            deployment_name=f"{PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE}/{PREFECT_CONSTANTS.DEPLOYMENT_NAMES.CALIBRATE}",
            days=days,
            base_parameters={
                "configuration": _configuration_for_deployment(configuration),
                "mode": mode,
                "sensor": sensor,
                "save_mode": save_mode,
                "metakernel": metakernel,
                "layer_data_format": layer_data_format,
            },
        )

    paths = _run_calibration(
        configuration,
        start_date,
        end_date,
        mode,
        sensor,
        save_mode,
        metakernel,
        layer_data_format,
    )

    json_paths = [path for path in paths if path.suffix.lower() == ".json"]

    if len(json_paths) == 0:
        raise RuntimeError(
            f"No calibration layers were generated for {start_date} to {end_date}."
        )

    if len(json_paths) > 1:
        logger.info(f"Calibration complete - {len(json_paths)} layers generated.")

    return paths
