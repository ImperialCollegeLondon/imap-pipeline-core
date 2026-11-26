import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated

import pandas as pd
from prefect import flow
from prefect.artifacts import create_table_artifact
from prefect.client.schemas.objects import FlowRun
from prefect.deployments import run_deployment
from prefect.runtime import flow_run
from prefect.variables import Variable
from pydantic import Field

from imap_mag.cli.generateLeinweberOffsets import (
    NoOffsetsProducedError,
    generate_leinweber_offsets,
)
from imap_mag.config import AppSettings, SaveMode
from imap_mag.db.Database import Database
from imap_mag.io import FileFinder
from imap_mag.io.file.HKDecodedPathHandler import HKDecodedPathHandler
from imap_mag.io.file.LeinweberOffsetsPathHandler import LeinweberOffsetsPathHandler
from imap_mag.util import DatetimeProvider, MAGSensor, ScienceMode, Subsystem
from mag_toolkit.calibration import Sensor
from prefect_server.calibrationFlowCommon import (
    APP_SETTINGS_DEFAULT_INSTANCE,
    PrefectScriptedLeinweberCalibrationConfig,
    SplitByDay,
    _configuration_for_deployment,
    _days_in_range,
    _resolve_matlab_repo_path,
)
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.durationUtils import parse_duration

logger = logging.getLogger(__name__)


@dataclass
class BackfillLeinweberRunParameters:
    """Sweep a rolling backfill window ending yesterday, sized from the
    ``leinweber_backfill_lookback_days`` Prefect variable (or overridden here)
    rather than an explicit date range."""

    backfill_lookback_days: Annotated[
        int | None,
        Field(
            json_schema_extra={
                "title": "Backfill lookback (days)",
                "description": "How many days (ending yesterday) to generate "
                "offsets for. Left unset, uses the "
                "leinweber_backfill_lookback_days Prefect variable.",
            }
        ),
    ] = None
    fail_on_missing_offsets: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Fail on missing offsets",
                "description": "If True, raise if MATLAB produces no offsets "
                "for a day. Defaults to False for a backfill sweep - science "
                "readiness is already checked per day, so one day in the "
                "window still not producing offsets shouldn't fail the run.",
            }
        ),
    ] = False
    force_regenerate: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Force regenerate",
                "description": "Generate and publish offsets even for days that "
                "already have them.",
            }
        ),
    ] = False


@dataclass
class LeinweberByDateRunParameters:
    """Generate offsets for an explicit day, or an inclusive range of days."""

    start_date: Annotated[
        datetime,
        Field(
            json_schema_extra={
                "title": "Start date / single day",
                "description": "Day (optionally with a time) to generate "
                "Leinweber offsets for. If end_date is not given, only this "
                "day is processed. The time of day only matters when "
                "configure_midpoint is USE_DATE_AS_IS.",
            }
        ),
    ]
    end_date: Annotated[
        datetime | None,
        Field(
            json_schema_extra={
                "title": "End date",
                "description": "Ending day (inclusive). If not specified, "
                "only the start date is processed.",
            }
        ),
    ] = None
    fail_on_missing_offsets: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Fail on missing offsets",
                "description": "If True (default), raise if MATLAB produces "
                "no offsets for a day in this explicit range.",
            }
        ),
    ] = True
    force_regenerate: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Force regenerate",
                "description": "Generate and publish offsets even for days that "
                "already have them.",
            }
        ),
    ] = True


def generate_leinweber_offsets_flow_run_name() -> str:
    parameters = flow_run.parameters
    run_parameters = parameters.get("run_parameters")
    calibration_configuration = parameters.get("calibration_configuration")
    # Prefect may hand this over as the model or as its raw dict form.
    if isinstance(calibration_configuration, dict):
        duration_str = calibration_configuration.get("duration")
    else:
        duration_str = getattr(calibration_configuration, "duration", None)
    if duration_str is None:
        duration_str = f"{_resolve_duration_hours(None)}h"

    start_date = getattr(run_parameters, "start_date", None)
    if start_date is None:
        # BackfillLeinweberRunParameters in use
        return f"Leinweber-offsets-auto-backfill-{duration_str}-window-duration"

    end_date = getattr(run_parameters, "end_date", None)
    date_str = start_date.strftime("%d-%m-%Y")
    if end_date and end_date != start_date:
        date_str = (
            f"{start_date.strftime('%d-%m-%Y')}-to-{end_date.strftime('%d-%m-%Y')}"
        )

    return f"Leinweber-offsets-{date_str}-{duration_str}"


def _resolve_duration_hours(duration_str: str | None) -> int:
    """Window width in whole hours: the explicit per-run override if given,
    otherwise the ``leinweber_duration_hours`` Prefect variable."""
    if duration_str is None:
        constants = PREFECT_CONSTANTS.GENERATE_LEINWEBER_OFFSETS
        return int(
            Variable.get(  # type: ignore[arg-type]
                constants.DURATION_HOURS_VARIABLE,
                default=constants.DEFAULT_DURATION_HOURS,
            )
        )
    return round(parse_duration(duration_str).total_seconds() / 3600)


def _resolve_backfill_lookback_days(override: int | None) -> int:
    """Backfill sweep length: the explicit per-run override if given, otherwise
    the ``leinweber_backfill_lookback_days`` Prefect variable."""
    if override is not None:
        return override
    constants = PREFECT_CONSTANTS.GENERATE_LEINWEBER_OFFSETS
    return int(
        Variable.get(  # type: ignore[arg-type]
            constants.BACKFILL_LOOKBACK_DAYS_VARIABLE,
            default=constants.DEFAULT_BACKFILL_LOOKBACK_DAYS,
        )
    )


def _start_of_day(moment: datetime) -> datetime:
    """Midnight at the start of ``moment``'s calendar day."""
    return datetime.combine(moment.date(), datetime.min.time())


def _end_of_day(moment: datetime) -> datetime:
    """The last representable instant (23:59:59.999999) of ``moment``'s calendar day."""
    return datetime.combine(moment.date(), datetime.max.time())


def _window_days(day: datetime, duration_hours: int) -> list[datetime]:
    """Calendar days the Leinweber window for ``day`` could touch, for any
    ``configure_midpoint`` strategy.

    MATLAB centres a ``duration_hours`` window on a midpoint that, depending on
    the strategy, can fall anywhere within ``day`` (e.g. 22:35 for the default
    USE_12_HRS_AFTER_PIVOT_PLATFORM_MOVEMENT, or any time for USE_DATE_AS_IS), so
    this is every day from the start of ``day`` minus half the duration to the
    end of ``day`` plus half the duration - always at least the day before, the
    day itself and the day after. Each is then checked for full-day coverage.
    """
    half = timedelta(hours=duration_hours / 2)
    return _days_in_range(
        _start_of_day(_start_of_day(day) - half),
        _start_of_day(_end_of_day(day) + half),
    )


def _file_has_gap_in_window(
    database: Database | None,
    filename: str,
    window_start: datetime,
    window_end: datetime,
) -> bool:
    """Whether the database's FileAnalysis record for ``filename`` shows the file
    failing to cover ``[window_start, window_end]`` - either via a recorded gap
    overlapping the window, or via the file's own recorded coverage
    (``first_timestamp``/``last_timestamp``) falling short of it.

    If there is no FileAnalysis record for the file, it is treated as a gap

    """
    if database is None:
        return False

    files = database.get_files(name=filename, deletion_date=None)
    if not files:
        return False

    analysis = database.get_file_analysis_by_file_id(files[0].id)

    if analysis is None:
        return True

    return analysis.gap_in_window(window_start, window_end)


def _science_ready(
    datastore_finder: FileFinder,
    database: Database | None,
    day: datetime,
    duration_hours: int,
    sensors: list[Sensor],
) -> bool:
    """Whether L1C norm science covers the full window, with no known gap
    overlapping it, for every requested sensor.

    Checks every calendar day the (conservatively estimated) window touches
    """

    for sensor in sensors:
        mag_sensor = MAGSensor(sensor.value.lower())
        for touched_day in _window_days(day, duration_hours):
            try:
                filename = datastore_finder.find_latest_science_by_date(
                    touched_day,
                    ScienceMode.Normal,
                    mag_sensor,
                    levels_to_search=["l1c"],
                )
            except FileNotFoundError:
                return False

            start_of_day = _start_of_day(touched_day)
            end_of_day = _end_of_day(touched_day)

            if _file_has_gap_in_window(database, filename, start_of_day, end_of_day):
                logger.info(
                    f"L1C norm science file {filename} has a gap "
                    f"overlapping the {duration_hours}h window around {day}."
                )
                return False
    return True


def _pivot_platform_complete(
    datastore_finder: FileFinder,
    database: Database | None,
    day: datetime,
    duration_hours: int,
) -> bool:
    """Whether IMAP-Lo pivot-platform-angle HK data covers the window, with
    no known gap overlapping it, for every calendar day the window touches.

    This is advisory only (unlike ``_science_ready``): generate_raw_leinweber_offset.m
    can extrapolate pivot-platform angles and also performs it's own pivot platform validation.
    """

    for touched_day in _window_days(day, duration_hours):
        handler = HKDecodedPathHandler(
            instrument=Subsystem.LO.short_name,
            descriptor="pivot-platform-angle",
            content_date=touched_day,
            extension="csv",
        )
        found = datastore_finder.find_latest_version_by_handler(
            handler, throw_if_not_found=False
        )

        # Give leeway for ILO Hk pivot platform angle files,
        # which do not have samples overlapping day boundaries
        start_of_day = _start_of_day(touched_day) + timedelta(minutes=1)
        end_of_day = _end_of_day(touched_day) - timedelta(minutes=1)
        if found is None:
            return False
        if _file_has_gap_in_window(database, found.name, start_of_day, end_of_day):
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
        handler = LeinweberOffsetsPathHandler(
            sensor=sensor.value.lower(),
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


def _create_offsets_table_artifact(paths: list[Path], start_date: datetime) -> None:
    """Publish a Prefect table artifact of the Leinweber offsets in ``paths``.

    Each path is a single-row CSV (one sensor/day) written by
    generate_raw_leinweber_offset.m's ``empty_offset_table``. The key is scoped to
    ``start_date`` so each date gets its own artifact (rather than every run of
    the flow, for any date, piling onto one ever-growing artifact) - re-running
    the flow for the same start_date still versions that date's artifact, so its
    history is browsable in the Prefect UI.
    """
    if not paths:
        return

    rows = []
    for path in paths:
        row = pd.read_csv(path).iloc[0]
        rows.append(
            {
                "Date": row["Date"],
                "Sensor": row["Sensor"],
                "Offset X (nT)": round(row["OffsetX"], 4),
                "Offset Y (nT)": round(row["OffsetY"], 4),
                "Offset Z (nT)": round(row["OffsetZ"], 4),
                "Dynamic MCS": round(row["Dynamic MCS"], 4),
                "Percent": round(row["Percent"], 4),
                "Verify Residual (nT)": (
                    round(row["VerifyResidual"], 4)
                    if pd.notna(row["VerifyResidual"])
                    else None
                ),
                "File": path.name,
            }
        )

    create_table_artifact(
        table=rows,
        key=f"{start_date:%Y-%m-%d}-leinweber-offsets",
        description="Leinweber (solar-wind) offsets produced by this flow run.",
    )


def _submit_leinweber_days_as_deployment_runs(
    deployment_name: str,
    days: list[datetime],
    fail_on_missing_offsets: bool,
    force_regenerate: bool,
    base_parameters: dict,
) -> list[FlowRun]:
    """Resubmit one deployment flow run per day so a date range fans out across
    workers.

    This is the Leinweber-flow analogue of
    ``calibrationFlowCommon._submit_days_as_deployment_runs`` -
    ``fail_on_missing_offsets`` and ``force_regenerate`` carry over the parent
    run's effective values`.
    """
    flow_runs: list[FlowRun] = []
    for day in days:
        flow_run_result: FlowRun = run_deployment(
            name=deployment_name,
            parameters={
                **base_parameters,
                "run_parameters": LeinweberByDateRunParameters(
                    start_date=day,
                    end_date=day,
                    fail_on_missing_offsets=fail_on_missing_offsets,
                    force_regenerate=force_regenerate,
                ),
                "split_by_day": False,
            },
            as_subflow=True,
            timeout=0,  # submit and return immediately; do not wait for the day to finish
        )
        logger.info(
            f"Submitted {deployment_name} run for {day} as flow run "
            f"'{flow_run_result.name}' ({flow_run_result.id})"
        )
        flow_runs.append(flow_run_result)
    return flow_runs


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.GENERATE_LEINWEBER_OFFSETS,
    log_prints=True,
    flow_run_name=generate_leinweber_offsets_flow_run_name,
)
def generate_leinweber_offsets_flow(
    run_parameters: Annotated[
        BackfillLeinweberRunParameters | LeinweberByDateRunParameters,
        Field(
            json_schema_extra={
                "title": "Run parameters",
                "description": "Either a rolling backfill window sized from "
                "config (BackfillLeinweberRunParameters), or an explicit day/"
                "date range to generate offsets for (LeinweberByDateRunParameters).",
                "position": 1,
            }
        ),
    ] = BackfillLeinweberRunParameters(),
    calibration_configuration: Annotated[
        PrefectScriptedLeinweberCalibrationConfig,
        Field(
            json_schema_extra={
                "title": "Calibration configuration",
                "description": "MATLAB repo, calibration matrix version, window "
                "duration, midpoint, verification and pivot-platform options.",
                "position": 2,
            }
        ),
    ] = PrefectScriptedLeinweberCalibrationConfig.build_with_defaults_from_app_settings(
        APP_SETTINGS_DEFAULT_INSTANCE
    ),
    sensors: list[Sensor] = [Sensor.MAGO, Sensor.MAGI],
    save_mode: SaveMode = SaveMode.LocalAndDatabase,
    split_by_day: SplitByDay = False,
) -> list[Path] | list[FlowRun]:
    """Generate daily Leinweber (solar-wind) offsets for a date range.

    Wraps ``imap-mag calibration leinweber``, running it once per
    day in the range and publishing each day's offsets to the datastore.

    Each day is skipped if:
    - Leinweber offsets are already published for that day, for every requested
      sensor - unless ``run_parameters.force_regenerate`` is set
    - The L1C norm science input does not yet cover the full window, or has known
      gap (per the database's file-analysis records) overlapping the window

    IMAP-Lo pivot-platform HK data is checked but only as a warning. MATLAB performs fuller validation.

    ``run_parameters`` selects the mode: ``BackfillLeinweberRunParameters`` sweeps
    a rolling backfill window ending yesterday, ``LeinweberByDateRunParameters``
    processes an explicit day or date range.
    """
    app_settings = AppSettings.create(is_prefect=True)

    duration_hours = _resolve_duration_hours(calibration_configuration.duration)
    logger.info(f"Window Duration: {duration_hours}h")

    # Log values this run actually uses
    effective_matrix_version = calibration_configuration.calibration_matrix_version
    effective_input_json_file = calibration_configuration.input_json_file

    logger.info(f"Calibration Matrix Version: {effective_matrix_version}")
    logger.info(f"Calibration Input File: {effective_input_json_file}")

    fail_on_missing_offsets = run_parameters.fail_on_missing_offsets
    force_regenerate = run_parameters.force_regenerate

    if isinstance(run_parameters, LeinweberByDateRunParameters):
        start_date = run_parameters.start_date
        end_date = run_parameters.end_date
        if end_date and end_date < start_date:
            raise ValueError(
                f"End date {end_date} cannot be before start date {start_date}."
            )
    else:
        lookback_days = _resolve_backfill_lookback_days(
            run_parameters.backfill_lookback_days
        )
        logger.info(f"Backfill Days: {lookback_days}")
        end_date = DatetimeProvider().yesterday(datetime)
        start_date = end_date - timedelta(days=lookback_days - 1)

    range_end = datetime.combine((end_date or start_date).date(), start_date.timetz())
    days = _days_in_range(start_date, range_end)

    if split_by_day and len(days) > 1:
        return _submit_leinweber_days_as_deployment_runs(
            deployment_name=f"{PREFECT_CONSTANTS.FLOW_NAMES.GENERATE_LEINWEBER_OFFSETS}/{PREFECT_CONSTANTS.DEPLOYMENT_NAMES.GENERATE_LEINWEBER_OFFSETS}",
            days=days,
            fail_on_missing_offsets=fail_on_missing_offsets,
            force_regenerate=force_regenerate,
            base_parameters={
                "sensors": sensors,
                "calibration_configuration": _configuration_for_deployment(
                    calibration_configuration
                ),
                "save_mode": save_mode,
            },
        )

    database = Database(app_settings) if app_settings.db_url else None
    datastore_finder = FileFinder(app_settings.data_store, database=database)

    produced: list[Path] = []
    pending_days: list[datetime] = []
    for day in days:
        if not force_regenerate:
            existing = _existing_offsets(datastore_finder, day, duration_hours, sensors)
            if existing is not None:
                logger.info(f"Leinweber offsets already published for {day}; skipping.")
                produced.extend(existing)
                continue

        if not _science_ready(datastore_finder, database, day, duration_hours, sensors):
            logger.info(
                f"L1C norm science not yet available (or incomplete) for all "
                f"sensors on {day}; skipping."
            )
            continue

        if not _pivot_platform_complete(
            datastore_finder, database, day, duration_hours
        ):
            logger.warning(
                f"IMAP-Lo pivot-platform data is missing or has known gaps for "
                f"the window around {day}; proceeding anyway"
            )

        pending_days.append(day)

    if pending_days:
        matlab_repo_path = _resolve_matlab_repo_path(
            calibration_configuration.matlab_repo, app_settings.work_folder
        )
        if matlab_repo_path is None:
            raise ValueError("matlab_repo is required to generate Leinweber offsets.")

        for day in pending_days:
            try:
                produced.extend(
                    generate_leinweber_offsets(
                        date=day,
                        matlab_repo=matlab_repo_path,
                        duration_hours=duration_hours,
                        sensors=sensors,
                        configure_midpoint=calibration_configuration.configure_midpoint,
                        calibration_matrix_version=calibration_configuration.calibration_matrix_version,
                        calibration_input_json_file=calibration_configuration.input_json_file,
                        verify=calibration_configuration.verify,
                        save_mode=save_mode,
                        cleanup_temp_files_after_run=calibration_configuration.cleanup_temp_files_after_run,
                        allow_pivot_platform_extrapolation=calibration_configuration.allow_pivot_platform_extrapolation,
                    )
                )
            except NoOffsetsProducedError:
                if fail_on_missing_offsets:
                    raise
                logger.warning(f"No Leinweber offsets were produced for {day}")
    else:
        logger.info(
            f"Nothing new to generate for {start_date} to {end_date} "
            f"({len(produced)} already published)."
        )

    logger.info(f"Leinweber offset generation complete - {len(produced)} file(s).")
    _create_offsets_table_artifact(produced, start_date)
    return produced
