import logging
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated

from prefect import flow
from prefect.client.schemas.objects import FlowRun
from prefect.deployments import run_deployment
from prefect.filesystems import LocalFileSystem
from prefect.runtime import flow_run
from prefect_github import GitHubRepository
from pydantic import Field

from imap_mag.cli.apply import FileType, apply
from imap_mag.cli.calibrate import Sensor, calibrate
from imap_mag.config import SaveMode
from imap_mag.config.AppSettings import AppSettings
from imap_mag.util import ReferenceFrame, ScienceMode
from mag_toolkit.calibration import (
    CalibrationLayer,
    CalibrationMethod,
)
from mag_toolkit.calibration.CalibrationConfig import (
    GradiometryConfig,
    ScriptedL2CalibrationConfig,
    SetQualityAndNaNConfig,
)
from prefect_server.constants import PREFECT_CONSTANTS

logger = logging.getLogger(__name__)


# Shared, self-documenting definition of the split_by_day flow parameter so the
# title/description show up consistently in the Prefect UI for every flow that
# supports it (calibrate, apply, calibrate-and-apply).
SplitByDay = Annotated[
    bool,
    Field(
        json_schema_extra={
            "title": "Split by day",
            "description": (
                "If true and a date range spanning more than one day is given, each "
                "day is resubmitted as its own deployment flow run so the range is "
                "processed in daily chunks that can run in parallel across multiple "
                "servers. If false (the default), the whole range runs sequentially "
                "within this single flow run."
            ),
        },
    ),
]


class PrefectScriptedL2CalibrationConfig(ScriptedL2CalibrationConfig):
    matlab_repo: LocalFileSystem | GitHubRepository | str | None = None


def _days_in_range(start_date: datetime, end_date: datetime | None) -> list[datetime]:
    """Return each day (inclusive) in the ``[start_date, end_date]`` range.

    A single day is returned when ``end_date`` is ``None`` or equal to ``start_date``.

    Args:
        start_date: First day of the range.
        end_date: Last day of the range (inclusive), or ``None`` for a single day.

    Returns:
        One ``datetime`` per day, preserving the time-of-day of ``start_date``.
    """
    effective_end = end_date or start_date
    num_days = (effective_end.date() - start_date.date()).days + 1
    return [start_date + timedelta(days=i) for i in range(num_days)]


def _submit_days_as_deployment_runs(
    deployment_name: str,
    days: list[datetime],
    base_parameters: dict,
) -> list[FlowRun]:
    """Resubmit one deployment flow run per day so a date range fans out across workers.

    Each per-day run is created with ``start_date == end_date`` for that day and
    ``split_by_day`` disabled, so a worker (potentially on a different server) processes
    exactly one day. Runs are submitted without waiting for completion (``timeout=0``)
    so all days are enqueued and picked up in parallel by the worker pool.

    Args:
        deployment_name: The ``"<flow-name>/<deployment-name>"`` to run for each day.
        days: The days to submit, one deployment run each.
        base_parameters: Parameters shared by every day. ``start_date``, ``end_date``
            and ``split_by_day`` are set per run and must not be included here.

    Returns:
        The created flow runs, one per day, in the same order as ``days``.
    """
    flow_runs: list[FlowRun] = []
    for day in days:
        flow_run_result: FlowRun = run_deployment(
            name=deployment_name,
            parameters={
                **base_parameters,
                "start_date": day,
                "end_date": day,
                "split_by_day": False,
            },
            as_subflow=True,
            timeout=0,  # submit and return immediately; do not wait for the day to finish
        )
        logger.info(
            f"Submitted {deployment_name} run for {day.date()} as flow run "
            f"'{flow_run_result.name}' ({flow_run_result.id})"
        )
        flow_runs.append(flow_run_result)
    return flow_runs


def _github_repo_name(repository_url: str) -> str:
    """Extract the repository name from a git/https clone URL.

    e.g. ``git@github.com:ImperialCollegeLondon/IMAP_MAG_Calibration.git`` ->
    ``IMAP_MAG_Calibration``.
    """
    name = re.split(r"[/:]", repository_url.rstrip("/"))[-1]
    if name.endswith(".git"):
        name = name[: -len(".git")]
    return name


def _load_matlab_repo_block(
    block_name: str,
) -> GitHubRepository | LocalFileSystem | None:
    """Load a MATLAB repo block by name, trying each supported block type."""
    for block_type in (GitHubRepository, LocalFileSystem):
        try:
            return block_type.load(block_name)
        except Exception:
            logger.debug(
                f"Block '{block_name}' is not a {block_type.__name__}, trying next type."
            )
    return None


def _resolve_matlab_repo_path(
    matlab_repo: "LocalFileSystem | GitHubRepository | str | None",
    work_folder: Path,
) -> Path | None:
    """Resolve the ``matlab_repo`` argument to a local path to the MATLAB code.

    ``matlab_repo`` may be a block name (str), a LocalFileSystem block (local path),
    a GitHubRepository block (pulled into a subfolder of the work folder named after
    the repo), or None. Raises if a provided repo cannot be found or pulled.
    """
    if not matlab_repo:
        return None

    block: LocalFileSystem | GitHubRepository | None = None
    if isinstance(matlab_repo, str):
        block = _load_matlab_repo_block(matlab_repo)
        if block is None:
            raise ValueError(
                f"Could not load a MATLAB repository block named '{matlab_repo}'."
            )
    else:
        block = matlab_repo

    if isinstance(block, LocalFileSystem):
        repo_path = Path(block.basepath)
        if not repo_path.is_dir():
            raise FileNotFoundError(
                f"LocalFileSystem MATLAB repository path does not exist: {repo_path}"
            )
        logger.info(f"Using local MATLAB calibration repository at {repo_path}")
        return repo_path

    if isinstance(block, GitHubRepository):
        repo_name = _github_repo_name(block.repository_url)
        target = work_folder / repo_name
        logger.info(
            f"Pulling MATLAB calibration repository {block.repository_url} into {target}"
        )

        # need to clear the target folder if it already exists, otherwise the pull will fail
        if target.exists():
            logger.info(f"Target folder {target} already exists, clearing it first")
            shutil.rmtree(target, ignore_errors=True)

        block.get_directory(local_path=str(target))
        if not target.is_dir():
            raise FileNotFoundError(
                f"Failed to pull MATLAB repository to {target} from {block.repository_url}."
            )
        return target

    raise TypeError(
        f"Unsupported matlab_repo type: {type(block).__name__}. Expected a "
        "LocalFileSystem block, GitHubRepository block, block name or None."
    )


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


def generate_apply_calibration_flow_run_name() -> str:
    parameters = flow_run.parameters
    layers: list[str] = parameters["layers"]
    start_date: datetime = parameters["start_date"]
    end_date = parameters.get("end_date")

    layers_str = ",".join(layers[:3])
    if len(layers) > 3:
        layers_str += f"...+{len(layers) - 3}"

    date_str = start_date.strftime("%d-%m-%Y")
    if end_date and end_date != start_date:
        date_str = (
            f"{start_date.strftime('%d-%m-%Y')}-to-{end_date.strftime('%d-%m-%Y')}"
        )

    return f"Applying-{layers_str}-for-{date_str}"


def generate_calibrate_and_apply_flow_run_name() -> str:
    parameters = flow_run.parameters
    start_date: datetime = parameters["start_date"]
    end_date = parameters.get("end_date")
    method: CalibrationMethod = parameters["configuration"].get_method()
    mode: ScienceMode = parameters["mode"]
    sensor: Sensor = parameters.get("sensor", Sensor.MAGO)

    date_str = start_date.strftime("%d-%m-%Y")
    if end_date and end_date != start_date:
        date_str = (
            f"{start_date.strftime('%d-%m-%Y')}-to-{end_date.strftime('%d-%m-%Y')}"
        )

    return f"Calibrating-and-applying-{date_str}-for-{sensor.value}-{mode.value}-with-{method.value}"


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE,
    log_prints=True,
    flow_run_name=generate_calibration_flow_run_name,
)
def calibrate_flow(
    start_date: datetime,
    configuration: PrefectScriptedL2CalibrationConfig
    | SetQualityAndNaNConfig
    | GradiometryConfig,
    end_date: datetime | None = None,
    mode: ScienceMode = ScienceMode.Normal,
    sensor: Sensor = Sensor.MAGO,
    save_mode: SaveMode = SaveMode.LocalAndDatabase,
    metakernel: Path | None = None,
    split_by_day: SplitByDay = False,
) -> list[Path] | list[FlowRun]:

    days = _days_in_range(start_date, end_date)
    if split_by_day and len(days) > 1:
        return _submit_days_as_deployment_runs(
            deployment_name=f"{PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE}/{PREFECT_CONSTANTS.DEPLOYMENT_NAMES.CALIBRATE}",
            days=days,
            base_parameters={
                "configuration": configuration,
                "mode": mode,
                "sensor": sensor,
                "save_mode": save_mode,
                "metakernel": metakernel,
            },
        )

    paths = _run_calibration(
        configuration, start_date, end_date, mode, sensor, save_mode, metakernel
    )
    return paths


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE_AND_APPLY,
    log_prints=True,
    flow_run_name=generate_calibrate_and_apply_flow_run_name,
)
def calibrate_and_apply_flow(
    configuration: PrefectScriptedL2CalibrationConfig
    | SetQualityAndNaNConfig
    | GradiometryConfig,
    start_date: datetime,
    end_date: datetime | None = None,
    mode: ScienceMode = ScienceMode.Normal,
    sensor: Sensor = Sensor.MAGO,
    offset_file_output_type: FileType = FileType.CDF,
    L2_output_type: FileType = FileType.CDF,
    save_mode: SaveMode = SaveMode.LocalAndDatabase,
    metakernel: Path | None = None,
    split_by_day: SplitByDay = False,
) -> None | list[FlowRun]:
    days = _days_in_range(start_date, end_date)
    if split_by_day and len(days) > 1:
        return _submit_days_as_deployment_runs(
            deployment_name=f"{PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE_AND_APPLY}/{PREFECT_CONSTANTS.DEPLOYMENT_NAMES.CALIBRATE_AND_APPLY}",
            days=days,
            base_parameters={
                "configuration": configuration,
                "mode": mode,
                "sensor": sensor,
                "offset_file_output_type": offset_file_output_type,
                "L2_output_type": L2_output_type,
                "save_mode": save_mode,
                "metakernel": metakernel,
            },
        )

    cal_layer_paths = _run_calibration(
        configuration, start_date, end_date, mode, sensor, save_mode, metakernel
    )

    layer = CalibrationLayer.from_file(cal_layer_paths[0])
    science_input = layer.metadata.science[0]
    apply(
        layers=[cal_layer_path.name for cal_layer_path in cal_layer_paths],
        start_date=start_date.replace(tzinfo=None),
        end_date=end_date.replace(tzinfo=None) if end_date else None,
        input=science_input,
        offset_file_output_type=offset_file_output_type.value,
        l2_output_type=L2_output_type.value,
        save_mode=save_mode,
        mode=mode,
    )


def _run_calibration(
    configuration, start_date, end_date, mode, sensor, save_mode, metakernel
):
    if type(configuration) is PrefectScriptedL2CalibrationConfig:
        app_settings = AppSettings()  # type: ignore
        # Pull/resolve the MATLAB code into the (stable) base work folder so it is
        # cloned once and reused across every day in the range. The resolved local
        # path (a plain string) replaces the block reference so that the config
        # crossing the JSON boundary matches the base ScriptedL2CalibrationConfig's
        # `matlab_repo: str` field.
        matlab_repo_path = _resolve_matlab_repo_path(
            configuration.matlab_repo, app_settings.work_folder
        )
        if matlab_repo_path is None:
            raise ValueError(
                "matlab_repo is required for the scripted-l2 calibration method."
            )
        # the lower level calibrate needs to remove the references to the prefect blocks and just have the path to the repo, so we update the configuration to have the path instead of the block reference
        configuration = configuration.model_copy(
            update={"matlab_repo": str(matlab_repo_path)}
        )

    cal_layer_paths: list[Path] = calibrate(
        start_date=start_date.replace(tzinfo=None),
        end_date=end_date.replace(tzinfo=None) if end_date else None,
        method=configuration.get_method(),
        mode=mode,
        sensor=sensor,
        configuration=configuration.model_dump_json() if configuration else None,
        save_mode=save_mode,
        metakernel=metakernel,
    )

    return cal_layer_paths


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.APPLY_CALIBRATION,
    log_prints=True,
    flow_run_name=generate_apply_calibration_flow_run_name,
)
def apply_flow(
    layers: list[str],
    start_date: datetime,
    end_date: datetime | None = None,
    mode: ScienceMode | None = None,
    science_input_file: str | None = None,
    offset_file_output_type: FileType = FileType.CDF,
    L2_output_type: FileType = FileType.CDF,
    save_mode: SaveMode = SaveMode.LocalAndDatabase,
    rotation_calibration_file_name: str | None = None,
    spice_metakernel: Path | None = None,
    reference_frames: list[ReferenceFrame] | None = [
        ReferenceFrame.GSE,
        ReferenceFrame.SRF,
    ],
    split_by_day: SplitByDay = False,
) -> None | list[FlowRun]:
    days = _days_in_range(start_date, end_date)
    if split_by_day and len(days) > 1:
        return _submit_days_as_deployment_runs(
            deployment_name=f"{PREFECT_CONSTANTS.FLOW_NAMES.APPLY_CALIBRATION}/{PREFECT_CONSTANTS.DEPLOYMENT_NAMES.APPLY_CALIBRATION}",
            days=days,
            base_parameters={
                "layers": layers,
                "mode": mode,
                "science_input_file": science_input_file,
                "offset_file_output_type": offset_file_output_type,
                "L2_output_type": L2_output_type,
                "save_mode": save_mode,
                "rotation_calibration_file_name": rotation_calibration_file_name,
                "spice_metakernel": spice_metakernel,
                "reference_frames": reference_frames,
            },
        )

    apply(
        layers,
        start_date=start_date.replace(tzinfo=None),
        end_date=end_date.replace(tzinfo=None) if end_date else None,
        mode=mode,
        input=science_input_file,
        offset_file_output_type=offset_file_output_type.value,
        l2_output_type=L2_output_type.value,
        save_mode=save_mode,
        rotation=Path(rotation_calibration_file_name)
        if rotation_calibration_file_name
        else None,
        spice_metakernel=spice_metakernel,
        reference_frames=reference_frames,
    )
