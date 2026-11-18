from datetime import datetime, timedelta
import logging
from pathlib import Path
from typing import Annotated

import pandas as pd
from prefect import flow
from prefect.artifacts import create_table_artifact
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
from imap_mag.db.Database import Database
from imap_mag.io import FileFinder
from imap_mag.io.file.HKDecodedPathHandler import HKDecodedPathHandler
from imap_mag.io.file.LeinweberOffsetsPathHandler import LeinweberOffsetsPathHandler
from imap_mag.util import DatetimeProvider, MAGSensor, ScienceMode, Subsystem
from mag_toolkit.calibration import Sensor
from prefect_server.calibrationFlowCommon import (
    DEFAULT_CALIBRATION_MATRIX_VERSION,
    DEFAULT_INPUT_JSON_FILE,
    DEFAULT_MATLAB_REPO_PATH,
    SplitByDay,
    _days_in_range,
    _resolve_matlab_repo_path,
    _submit_days_as_deployment_runs,
    _swap_matlab_repo_for_block_ref,
)
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.durationUtils import parse_duration

logger = logging.getLogger(__name__)

# generate-leinweber-offsets is another MATLAB tool under the calibrate umbrella,
# so it shares calibrate's calibration_matrix_version/input_json_file defaults
# (DEFAULT_CALIBRATION_MATRIX_VERSION/DEFAULT_INPUT_JSON_FILE above) rather than
# having its own. Only backfill_lookback_days is genuinely leinweber-specific.
DEFAULT_BACKFILL_LOOKBACK_DAYS = (
    CalibrationCommandConfig().leinweber.backfill_lookback_days
)

def _assume_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.replace(tzinfo=None)


def generate_leinweber_offsets_flow_run_name() -> str:
    parameters = flow_run.parameters
    start_date = parameters.get("start_date")
    duration_str = parameters.get("duration_str", "24h")

    if start_date is None:
        # No explicit date(s): the flow sweeps a rolling backfill window computed
        # from config, which isn't known yet at this point - label generically.
        return f"Leinweber-offsets-auto-backfill-{duration_str}"

    end_date = parameters.get("end_date")
    date_str = start_date.strftime("%d-%m-%Y")
    if end_date and end_date != start_date:
        date_str = (
            f"{start_date.strftime('%d-%m-%Y')}-to-{end_date.strftime('%d-%m-%Y')}"
        )

    return f"Leinweber-offsets-{date_str}-{duration_str}"


def _window_bounds(day: datetime, duration_hours: int) -> tuple[datetime, datetime]:
    """Precise (not rounded to whole days) start/end of the conservative Leinweber
    window for ``day``, for any ``configure_midpoint`` strategy. See ``_window_days``
    for the whole-calendar-day version of this."""
    day_midday = day.replace(hour=12, minute=0, second=0, microsecond=0)
    # The midpoint could fall anywhere within `day` depending on configure_midpoint,
    # not just at noon - so even a short duration must keep the window at least 24h
    # wide (centred on noon) to stay conservative across every midpoint placement.
    half = timedelta(hours=max(duration_hours, 24) / 2)
    return (
        day_midday - half - timedelta(minutes=1),
        day_midday + half + timedelta(minutes=1),
    )


def _window_days(day: datetime, duration_hours: int) -> list[datetime]:
    """Calendar days that could plausibly be touched by the Leinweber window for
    ``day``, for any ``configure_midpoint`` strategy.

    A conservative science readiness check for L1C norm science
    """
    window_start, window_end = _window_bounds(day, duration_hours)

    first_day = window_start.replace(hour=0, minute=0, second=0, microsecond=0)
    last_day = window_end.replace(hour=0, minute=0, second=0, microsecond=0)

    return _days_in_range(first_day, last_day)


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

    """
    if database is None:
        return False

    files = database.get_files(name=filename, deletion_date=None)
    if not files:
        return False

    analysis = database.get_file_analysis_by_file_id(files[0].id)
    if analysis is None:
        return False

    if analysis.first_timestamp is None or analysis.last_timestamp is None:
        # No usable timestamp range recorded at all - coverage can't be confirmed.
        return True

    if _assume_utc(analysis.first_timestamp) > window_start or _assume_utc(analysis.last_timestamp) < window_end:
        return True

    for gap in analysis.gaps or []:
        gap_start = _assume_utc(datetime.fromisoformat(gap["start_time"]))
        gap_end = _assume_utc(datetime.fromisoformat(gap["end_time"]))
        if gap_start < window_end and gap_end > window_start:
            return True
    return False


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

            
            # Give some leeway at the start and end of the day, to allow for occasional small gaps at the 
            # beginning and end of the day that shouldn't be a blocker for Leinweber offset generation
            start_of_day = datetime.combine(touched_day.date(), datetime.min.time()) + timedelta(minutes=1)
            end_of_day = datetime.combine(touched_day.date(), datetime.max.time()) - timedelta(minutes=1)

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
        start_of_day = datetime.combine(touched_day.date(), datetime.min.time()) + timedelta(minutes=1)
        end_of_day = datetime.combine(touched_day.date(), datetime.max.time()) - timedelta(minutes=1)
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
    duration_str: Annotated[
        str,
        Field(
            json_schema_extra={
                "title": "Duration",
                "description": "Duration for the analysis window. Sub-hour fields will be rounded to the nearest hour",
                "position": 3,
            }
        ),
    ] = "24h",
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

    Wraps ``imap-mag calibration leinweber``, running it once per
    day in the range and publishing each day's offsets to the datastore.

    Each day is skipped if:
    - Leinweber offsets are already published for that day, for every requested
      sensor - unless ``force_regenerate`` is set
    - The L1C norm science input does not yet cover the full window, or has known
      gap (per the database's file-analysis records) overlapping the window

    IMAP-Lo pivot-platform HK data is checked but only as a warning. MATLAB performs fuller validation.

    When ``start_date`` is omitted it defaults to a rolling ``backfill_lookback_days``
    """
    app_settings = AppSettings()  # type: ignore

    # Everything downstream (the window check, the published-offsets lookup, and
    # the MATLAB call) works in whole hours - the file-name convention embeds an
    # integer hour count, so a sub-hour duration_str is rounded here, once, rather
    # than at each place that reads it.
    duration_hours = round(parse_duration(duration_str).total_seconds() / 3600)

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
                "duration_str": duration_str,
                "sensors": sensors,
                "matlab_repo": _swap_matlab_repo_for_block_ref(matlab_repo),
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

    database = Database() if Database.get_environment_url() else None
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
            matlab_repo, app_settings.work_folder
        )
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
    else:
        logger.info(
            f"Nothing new to generate for {start_date} to {end_date} "
            f"({len(produced)} already published)."
        )

    logger.info(f"Leinweber offset generation complete - {len(produced)} file(s).")
    _create_offsets_table_artifact(produced, start_date)
    return produced
