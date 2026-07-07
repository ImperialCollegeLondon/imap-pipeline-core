import logging
import re
from datetime import datetime
from pathlib import Path

from prefect import flow
from prefect.filesystems import LocalFileSystem
from prefect.runtime import flow_run
from prefect_github import GitHubRepository

from imap_mag.cli.apply import FileType, apply
from imap_mag.cli.calibrate import Sensor, calibrate, gradiometry
from imap_mag.config import SaveMode
from imap_mag.config.AppSettings import AppSettings
from imap_mag.config.CalibrationConfig import (
    CalibrationConfig,
    ScriptedL2CalibrationConfig,
)
from imap_mag.util import ReferenceFrame, ScienceMode
from mag_toolkit.calibration import (
    CalibrationLayer,
    CalibrationMethod,
    DatastoreAccessMode,
)
from prefect_server.constants import PREFECT_CONSTANTS

logger = logging.getLogger(__name__)


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
    if matlab_repo is None:
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
    match flow_run.flow_name:
        case PREFECT_CONSTANTS.FLOW_NAMES.GRADIOMETRY:
            method_name = CalibrationMethod.GRADIOMETER
        case _:
            method_name = flow_run.parameters["method"]

    parameters = flow_run.parameters
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
    name=PREFECT_CONSTANTS.FLOW_NAMES.GRADIOMETRY,
    log_prints=True,
    flow_run_name=generate_calibration_flow_run_name,
)
def gradiometry_flow(
    start_date: datetime,
    mode: ScienceMode,
    kappa: float = 0.0,
    sc_interference_threshold: float = 10.0,
):
    """
    Run the gradiometry calibration.
    """

    gradiometry(
        start_date=start_date,
        mode=mode,
        kappa=kappa,
        sc_interference_threshold=sc_interference_threshold,
        save_mode=SaveMode.LocalAndDatabase,
    )


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE,
    log_prints=True,
    flow_run_name=generate_calibration_flow_run_name,
)
def calibrate_flow(
    start_date: datetime,
    end_date: datetime | None = None,
    method: CalibrationMethod = CalibrationMethod.KEPKO,
    mode: ScienceMode = ScienceMode.Normal,
    configuration: ScriptedL2CalibrationConfig | CalibrationConfig | None = None,
    sensor: Sensor = Sensor.MAGO,
    save_mode: SaveMode = SaveMode.LocalAndDatabase,
    metakernel: Path | None = None,
    matlab_repo: LocalFileSystem | GitHubRepository | str | None = None,
    datastore_access_mode: DatastoreAccessMode = DatastoreAccessMode.READ_DIRECTLY,
) -> list[Path]:
    """Calibrate for a date or date range. Returns a list of calibration layer paths.

    Args:
        start_date: First date to calibrate.
        end_date: Last date to calibrate (inclusive). If None, only start_date.
        method: Calibration method to run. Only SCRIPTED_L2_CALIBRATION uses the
            metakernel/matlab_repo/datastore_access_mode arguments.
        mode: Science mode (norm/burst) to calibrate.
        configuration: Calibration configuration. A ScriptedL2CalibrationConfig for
            the scripted-l2 method, otherwise a CalibrationConfig (or None).
        sensor: Sensor to calibrate (defaults to MAGo).
        save_mode: Whether to save locally only or also index to the database.
        metakernel: Filename of the SPICE metakernel to use for the scripted-l2
            method. Treated like ``spice_metakernel`` in ``apply_flow``: if provided
            it must exist (in the datastore's spice/mk folder); if None and the
            method is scripted-l2 one is generated.
        matlab_repo: Where to acquire the MATLAB calibration code from for the
            scripted-l2 method. A LocalFileSystem block (local path), a
            GitHubRepository block (pulled into the work folder), the name of such
            a block, or None for the other methods.
        datastore_access_mode: For scripted-l2, whether MATLAB reads the datastore
            directly or from a sparse copy built in the work folder.
    """
    matlab_repo_path: Path | None = None
    if method == CalibrationMethod.SCRIPTED_L2_CALIBRATION:
        app_settings = AppSettings()  # type: ignore
        # Pull/resolve the MATLAB code into the (stable) base work folder so it is
        # cloned once and reused across every day in the range.
        matlab_repo_path = _resolve_matlab_repo_path(
            matlab_repo, app_settings.work_folder
        )
        if matlab_repo_path is None:
            raise ValueError(
                "matlab_repo is required for the scripted-l2 calibration method."
            )

    return calibrate(
        start_date=start_date,
        end_date=end_date,
        method=method,
        mode=mode,
        sensor=sensor,
        configuration=configuration.model_dump_json() if configuration else None,
        save_mode=save_mode,
        metakernel=metakernel,
        matlab_repo_path=matlab_repo_path,
        datastore_access_mode=datastore_access_mode,
    )


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE_AND_APPLY,
    log_prints=True,
    flow_run_name=generate_calibrate_and_apply_flow_run_name,
)
def calibrate_and_apply_flow(
    start_date: datetime,
    end_date: datetime | None = None,
    method: CalibrationMethod = CalibrationMethod.KEPKO,
    configuration: ScriptedL2CalibrationConfig | CalibrationConfig | None = None,
    mode: ScienceMode = ScienceMode.Normal,
    sensor: Sensor = Sensor.MAGO,
    offset_file_output_type: FileType = FileType.CDF,
    L2_output_type: FileType = FileType.CDF,
    save_mode: SaveMode = SaveMode.LocalAndDatabase,
    metakernel: Path | None = None,
    matlab_repo: LocalFileSystem | GitHubRepository | str | None = None,
    datastore_access_mode: DatastoreAccessMode = DatastoreAccessMode.READ_DIRECTLY,
):
    """
    Calibrate and apply the calibration in one flow, for a date or date range.

    Accepts all the options available to ``calibrate_flow`` (passed through to it
    unchanged), plus the apply-specific output type options below.

    Args:
        offset_file_output_type: File type for the apply step's offset file output.
        L2_output_type: File type for the apply step's L2 output.
        metakernel: Filename of the SPICE metakernel to use for the scripted-l2
            method. See ``calibrate_flow``.
        matlab_repo: Where to acquire the MATLAB calibration code from for the
            scripted-l2 method. See ``calibrate_flow``.
        datastore_access_mode: For scripted-l2, whether MATLAB reads the datastore
            directly or from a sparse copy built in the work folder.
    """
    matlab_repo_path: Path | None = None
    if method == CalibrationMethod.SCRIPTED_L2_CALIBRATION:
        app_settings = AppSettings()  # type: ignore
        # Pull/resolve the MATLAB code into the (stable) base work folder so it is
        # cloned once and reused across every day in the range.
        matlab_repo_path = _resolve_matlab_repo_path(
            matlab_repo, app_settings.work_folder
        )
        if matlab_repo_path is None:
            raise ValueError(
                "matlab_repo is required for the scripted-l2 calibration method."
            )

    cal_layer_paths: list[Path] = calibrate(
        start_date=start_date,
        end_date=end_date,
        method=method,
        mode=mode,
        sensor=sensor,
        configuration=configuration.model_dump_json() if configuration else None,
        save_mode=save_mode,
        metakernel=metakernel,
        matlab_repo_path=matlab_repo_path,
        datastore_access_mode=datastore_access_mode,
    )

    layer = CalibrationLayer.from_file(cal_layer_paths[0])
    science_input = layer.metadata.science[0]
    apply(
        layers=[cal_layer_path.name for cal_layer_path in cal_layer_paths],
        start_date=start_date,
        end_date=end_date,
        input=science_input,
        offset_file_output_type=offset_file_output_type.value,
        l2_output_type=L2_output_type.value,
        save_mode=save_mode,
        mode=mode,
    )


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
    """Apply calibration layers for a date or date range.

    Args:
        layers: Layer filenames or glob patterns (e.g. ["*noop*"], ["*"]).
        start_date: Start date for processing.
        end_date: End date (inclusive). If None, only start_date is processed.
        mode: Science mode (norm/burst) for discovering science files when file is None.
        science_input_file: Science filename. If None, discovered using mode and date.
        save_mode: Where to save output files.
    """
    apply(
        layers,
        start_date=start_date,
        end_date=end_date,
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
