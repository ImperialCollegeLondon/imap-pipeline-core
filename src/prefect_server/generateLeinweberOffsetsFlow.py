from datetime import date, datetime
from pathlib import Path
from typing import Annotated

from prefect import flow
from prefect.client.schemas.objects import FlowRun
from prefect.filesystems import LocalFileSystem
from prefect.runtime import flow_run
from prefect_github import GitHubRepository
from pydantic import Field

from imap_mag.cli.generateLeinweberOffsets import generate_leinweber_offsets
from imap_mag.config import AppSettings, SaveMode
from mag_toolkit.calibration import Sensor
from prefect_server.calibrationFlowCommon import (
    DEFAULT_CALIBRATION_MATRIX_VERSION,
    DEFAULT_INPUT_JSON_FILE,
    SplitByDay,
    _days_in_range,
    _resolve_matlab_repo_path,
    _submit_days_as_deployment_runs,
    logger,
)
from prefect_server.constants import PREFECT_CONSTANTS


def generate_leinweber_offsets_flow_run_name() -> str:
    parameters = flow_run.parameters
    start_date: date = parameters["start_date"]
    end_date = parameters.get("end_date")
    duration_hours = parameters.get("duration_hours", 24)

    date_str = start_date.strftime("%d-%m-%Y")
    if end_date and end_date != start_date:
        date_str = (
            f"{start_date.strftime('%d-%m-%Y')}-to-{end_date.strftime('%d-%m-%Y')}"
        )

    return f"Leinweber-offsets-{date_str}-{duration_hours}hr"


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.GENERATE_LEINWEBER_OFFSETS,
    log_prints=True,
    flow_run_name=generate_leinweber_offsets_flow_run_name,
)
def generate_leinweber_offsets_flow(
    start_date: Annotated[
        date,
        Field(
            json_schema_extra={
                "title": "Start date / single day",
                "description": "Day to generate Leinweber offsets for. If end date "
                "is not specified, only this day is processed.",
                "position": 1,
            }
        ),
    ],
    end_date: Annotated[
        date | None,
        Field(
            json_schema_extra={
                "title": "End date",
                "description": "Ending day (inclusive). If not specified, only the "
                "start date is processed.",
                "position": 2,
            }
        ),
    ] = None,
    duration_hours: int = 24,
    sensors: list[Sensor] = [Sensor.MAGO, Sensor.MAGI],
    matlab_repo: LocalFileSystem | GitHubRepository | str | None = None,
    calibration_matrix_version: int = DEFAULT_CALIBRATION_MATRIX_VERSION,
    calibration_input_json_file: str = DEFAULT_INPUT_JSON_FILE,
    verify: bool = True,
    save_mode: SaveMode = SaveMode.LocalAndDatabase,
    cleanup_temp_files_after_run: bool = True,
    split_by_day: SplitByDay = False,
) -> list[Path] | list[FlowRun]:
    """Generate daily Leinweber (solar-wind) offsets for a date range.

    Wraps ``imap-mag calibration generate-leinweber-offsets``, running it once per
    day in the range and publishing each day's offsets to the datastore.
    """
    if end_date and end_date < start_date:
        raise ValueError(
            f"End date {end_date} cannot be before start date {start_date}."
        )

    days = _days_in_range(start_date, end_date)
    if split_by_day and len(days) > 1:
        return _submit_days_as_deployment_runs(
            deployment_name=f"{PREFECT_CONSTANTS.FLOW_NAMES.GENERATE_LEINWEBER_OFFSETS}/{PREFECT_CONSTANTS.DEPLOYMENT_NAMES.GENERATE_LEINWEBER_OFFSETS}",
            days=days,
            base_parameters={
                "duration_hours": duration_hours,
                "sensors": sensors,
                "matlab_repo": matlab_repo,
                "calibration_matrix_version": calibration_matrix_version,
                "calibration_input_json_file": calibration_input_json_file,
                "verify": verify,
                "save_mode": save_mode,
                "cleanup_temp_files_after_run": cleanup_temp_files_after_run,
            },
        )

    app_settings = AppSettings()  # type: ignore
    matlab_repo_path = _resolve_matlab_repo_path(matlab_repo, app_settings.work_folder)
    if matlab_repo_path is None:
        raise ValueError("matlab_repo is required to generate Leinweber offsets.")

    produced: list[Path] = []
    for day in days:
        produced.extend(
            generate_leinweber_offsets(
                date=datetime.combine(day, datetime.min.time()),
                matlab_repo=matlab_repo_path,
                duration_hours=duration_hours,
                sensors=sensors,
                calibration_matrix_version=calibration_matrix_version,
                calibration_input_json_file=calibration_input_json_file,
                verify=verify,
                save_mode=save_mode,
                cleanup_temp_files_after_run=cleanup_temp_files_after_run,
            )
        )

    if not produced:
        raise RuntimeError(
            f"No Leinweber offsets were generated for {start_date} to {end_date}."
        )

    logger.info(f"Leinweber offset generation complete - {len(produced)} file(s).")
    return produced
