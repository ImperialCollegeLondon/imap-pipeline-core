import logging
import re
import shutil
from datetime import datetime
from pathlib import Path

from prefect import flow
from prefect.filesystems import LocalFileSystem
from prefect.runtime import flow_run
from prefect_github import GitHubRepository

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


class PrefectScriptedL2CalibrationConfig(ScriptedL2CalibrationConfig):
    matlab_repo: LocalFileSystem | GitHubRepository | str | None = None


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
    method: CalibrationMethod = parameters["method"]
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
) -> list[Path]:

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
):
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
):
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
