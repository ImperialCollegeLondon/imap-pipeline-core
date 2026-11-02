from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated

from prefect import flow
from prefect.client.schemas.objects import FlowRun
from prefect.filesystems import LocalFileSystem
from prefect.runtime import flow_run
from prefect_github import GitHubRepository
from pydantic import Field

from imap_mag.cli.generateLeinweberOffsets import (
    ConfigureMidpoint,
    generate_leinweber_offsets,
)
from imap_mag.config import AppSettings, CalibrationCommandConfig, SaveMode
from imap_mag.io import FileFinder
from imap_mag.io.file.CalculatedOffsetsPathHandler import (
    LEINWEBER,
    CalculatedOffsetsPathHandler,
)
from imap_mag.util import DatetimeProvider, MAGSensor, ScienceMode
from mag_toolkit.calibration import Sensor
from prefect_server.calibrationFlowCommon import (
    DEFAULT_CALIBRATION_MATRIX_VERSION,
    DEFAULT_INPUT_JSON_FILE,
    DEFAULT_MATLAB_REPO_PATH,
    SplitByDay,
    _days_in_range,
    _resolve_matlab_repo_path,
    _submit_days_as_deployment_runs,
    logger,
)
from prefect_server.constants import PREFECT_CONSTANTS

# generate-leinweber-offsets is another MATLAB tool under the calibrate umbrella,
# so it shares calibrate's calibration_matrix_version/input_json_file defaults
# (DEFAULT_CALIBRATION_MATRIX_VERSION/DEFAULT_INPUT_JSON_FILE above) rather than
# having its own. Only backfill_lookback_days is genuinely leinweber-specific.
DEFAULT_BACKFILL_LOOKBACK_DAYS = (
    CalibrationCommandConfig().leinweber.backfill_lookback_days
)


def generate_leinweber_offsets_flow_run_name() -> str:
    parameters = flow_run.parameters
    start_date = parameters.get("start_date")
    duration_hours = parameters.get("duration_hours", 24)

    if start_date is None:
        # No explicit date(s): the flow sweeps a rolling backfill window computed
        # from config, which isn't known yet at this point - label generically.
        return f"Leinweber-offsets-auto-backfill-{duration_hours}hr"

    end_date = parameters.get("end_date")
    date_str = start_date.strftime("%d-%m-%Y")
    if end_date and end_date != start_date:
        date_str = (
            f"{start_date.strftime('%d-%m-%Y')}-to-{end_date.strftime('%d-%m-%Y')}"
        )

    return f"Leinweber-offsets-{date_str}-{duration_hours}hr"


def _window_days(day: datetime, duration_hours: int) -> list[datetime]:
    """Calendar days that could plausibly be touched by the Leinweber window for
    ``day``, for any ``configure_midpoint`` strategy.

    A conservative science readiness check for L1C norm science
    """
    day_midnight = day.replace(hour=0, minute=0, second=0, microsecond=0)
    half = timedelta(hours=duration_hours / 2)
    window_start = day_midnight - half - timedelta(minutes=1)
    window_end = day_midnight + timedelta(hours=24) + half + timedelta(minutes=1)

    first_day = window_start.replace(hour=0, minute=0, second=0, microsecond=0)
    last_day = window_end.replace(hour=0, minute=0, second=0, microsecond=0)

    num_days = (last_day - first_day).days + 1
    return [first_day + timedelta(days=i) for i in range(num_days)]


def _science_ready(
    datastore_finder: FileFinder,
    day: datetime,
    duration_hours: int,
    sensors: list[Sensor],
) -> bool:
    """Whether L1C norm science covers the full window for every requested sensor.

    Checks every calendar day the (conservatively estimated) window touches
    """
    for sensor in sensors:
        mag_sensor = MAGSensor(sensor.value.lower())
        for touched_day in _window_days(day, duration_hours):
            try:
                datastore_finder.find_latest_science_by_date(
                    touched_day, ScienceMode.Normal, mag_sensor
                )
            except FileNotFoundError:
                return False
    return True


def _existing_offsets(
    datastore_finder: FileFinder,
    day: datetime,
    duration_hours: int,
    sensors: list[Sensor],
) -> list[Path] | None:
    """Already-published offset files for this day, or None if any sensor is missing one."""
    existing: list[Path] = []
    for sensor in sensors:
        handler = CalculatedOffsetsPathHandler(
            sensor=sensor.value.lower(),
            offset_type=LEINWEBER,
            content_date=day,
            duration_hours=duration_hours,
        )
        found = datastore_finder.find_latest_version_by_handler(
            handler, throw_if_not_found=False
        )
        if found is None:
            return None
        existing.append(found)
    return existing


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.GENERATE_LEINWEBER_OFFSETS,
    log_prints=True,
    flow_run_name=generate_leinweber_offsets_flow_run_name,
)
def generate_leinweber_offsets_flow(
    start_date: Annotated[
        datetime | None,
        Field(
            json_schema_extra={
                "title": "Start date / single day",
                "description": "Day (optionally with a time) to generate Leinweber "
                "offsets for. If end date is not specified, only this day is "
                "processed. The time of day only matters when configure_midpoint "
                "is USE_DATE_AS_IS. If omitted "
                "entirely, defaults to a backfill window ending yesterday.",
                "position": 1,
            }
        ),
    ] = None,
    end_date: Annotated[
        datetime | None,
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
    matlab_repo: LocalFileSystem
    | GitHubRepository
    | str
    | None = DEFAULT_MATLAB_REPO_PATH,
    configure_midpoint: Annotated[
        ConfigureMidpoint,
        Field(
            json_schema_extra={
                "title": "Configure midpoint",
                "description": "How generate_raw_leinweber_offset derives its "
                "analysis midpoint from the date: USE_DATE_AS_IS uses the date "
                "(and time) exactly as given; USE_MID_DAY uses noon on that day; "
                "USE_12_HRS_AFTER_PIVOT_PLATFORM_MOVEMENT (MATLAB's own default) "
                "uses 12 hours after the IMAP-Lo pivot-platform movement time of "
                "day.",
            }
        ),
    ] = ConfigureMidpoint.USE_12_HRS_AFTER_PIVOT_PLATFORM_MOVEMENT,
    calibration_matrix_version: int = DEFAULT_CALIBRATION_MATRIX_VERSION,
    calibration_input_json_file: str = DEFAULT_INPUT_JSON_FILE,
    backfill_lookback_days: Annotated[
        int | None,
        Field(
            json_schema_extra={
                "title": "Backfill lookback (days)",
                "description": "Only used when start_date is omitted: how many "
                "days (ending yesterday) to generate offsets for. Left unset, uses the configured default "
                "(calibrate.leinweber.backfill_lookback_days, currently "
                f"{DEFAULT_BACKFILL_LOOKBACK_DAYS}).",
            }
        ),
    ] = None,
    verify: bool = True,
    save_mode: SaveMode = SaveMode.LocalAndDatabase,
    cleanup_temp_files_after_run: bool = True,
    split_by_day: SplitByDay = False,
    force_regenerate: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Force regenerate",
                "description": "Generate and publish offsets even for days that "
                "already have them. The normal content-hash dedup still applies.",
            }
        ),
    ] = False,
    allow_pivot_platform_extrapolation: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Allow pivot-platform extrapolation",
                "description": "If False (default), generate_raw_leinweber_offset "
                "skips a sensor/day rather than correcting it with extrapolated "
                "IMAP-Lo pivot-platform angles when the available pivot-platform "
                "data doesn't cover the window.",
            }
        ),
    ] = False,
) -> list[Path] | list[FlowRun]:
    """Generate daily Leinweber (solar-wind) offsets for a date range.

    Wraps ``imap-mag calibration generate-leinweber-offsets``, running it once per
    day in the range and publishing each day's offsets to the datastore.

    Each day is skipped rather than run when either:
    - Leinweber offsets are already published for that day, for every requested
      sensor - unless ``force_regenerate`` is set, which generates (and publishes,
      subject to the usual content-hash dedup) regardless.
    - The L1C norm science input does not yet cover the full window for every
      requested sensor

    When ``start_date`` is omitted (e.g. the daily schedule, which passes no
    parameters), it defaults to a rolling backfill window ending yesterday rather
    than just yesterday - the window size is ``backfill_lookback_days`` if given, else the configured
    default (``calibrate.leinweber.backfill_lookback_days``).
    """
    app_settings = AppSettings()  # type: ignore

    if start_date is None:
        lookback_days = (
            backfill_lookback_days
            if backfill_lookback_days is not None
            else app_settings.calibrate.leinweber.backfill_lookback_days
        )
        end_date = end_date or DatetimeProvider().yesterday(datetime)
        start_date = end_date - timedelta(days=lookback_days - 1)

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
                "configure_midpoint": configure_midpoint,
                "calibration_matrix_version": calibration_matrix_version,
                "calibration_input_json_file": calibration_input_json_file,
                "backfill_lookback_days": backfill_lookback_days,
                "verify": verify,
                "save_mode": save_mode,
                "cleanup_temp_files_after_run": cleanup_temp_files_after_run,
                "force_regenerate": force_regenerate,
                "allow_pivot_platform_extrapolation": allow_pivot_platform_extrapolation,
            },
        )

    datastore_finder = FileFinder(app_settings.data_store)

    produced: list[Path] = []
    pending_days: list[datetime] = []
    for day in days:
        if not force_regenerate:
            existing = _existing_offsets(datastore_finder, day, duration_hours, sensors)
            if existing is not None:
                logger.info(f"Leinweber offsets already published for {day}; skipping.")
                produced.extend(existing)
                continue

        if not _science_ready(datastore_finder, day, duration_hours, sensors):
            logger.info(
                f"L1C norm science not yet available for all sensors on {day}; "
                "skipping for now (will be picked up by a later run)."
            )
            continue

        pending_days.append(day)

    if not pending_days:
        logger.info(
            f"Nothing to generate for {start_date} to {end_date} "
            f"({len(produced)} already published)."
        )
        return produced

    matlab_repo_path = _resolve_matlab_repo_path(matlab_repo, app_settings.work_folder)
    if matlab_repo_path is None:
        raise ValueError("matlab_repo is required to generate Leinweber offsets.")

    for day in pending_days:
        produced.extend(
            generate_leinweber_offsets(
                date=day,
                matlab_repo=matlab_repo_path,
                duration_hours=duration_hours,
                sensors=sensors,
                configure_midpoint=configure_midpoint,
                calibration_matrix_version=calibration_matrix_version,
                calibration_input_json_file=calibration_input_json_file,
                verify=verify,
                save_mode=save_mode,
                cleanup_temp_files_after_run=cleanup_temp_files_after_run,
                allow_pivot_platform_extrapolation=allow_pivot_platform_extrapolation,
            )
        )

    logger.info(f"Leinweber offset generation complete - {len(produced)} file(s).")
    return produced
