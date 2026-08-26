import logging
import re
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Annotated

from prefect.client.schemas.objects import FlowRun
from prefect.deployments import run_deployment
from prefect.filesystems import LocalFileSystem
from prefect_github import GitHubRepository
from pydantic import Field

from imap_mag.cli.calibrate import calibrate
from imap_mag.config.AppSettings import AppSettings
from mag_toolkit.calibration import LayerDataFormat
from mag_toolkit.calibration.CalibrationConfig import (
    GradiometryConfig,
    ScriptedL2CalibrationConfig,
    SetQualityAndNaNConfig,
)

logger = logging.getLogger(__name__)
DEFAULT_INPUT_JSON_FILE = "+calibration/calibration/template.json"
DEFAULT_MATLAB_REPO_PATH = "/app/matlab/calibration"
DEFAULT_CALIBRATION_MATRIX_VERSION = 9

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


def _days_in_range(start_date: date, end_date: date | None) -> list[date]:
    """Return each day (inclusive) in the ``[start_date, end_date]`` range.

    A single day is returned when ``end_date`` is ``None`` or equal to ``start_date``.

    Args:
        start_date: First day of the range.
        end_date: Last day of the range (inclusive), or ``None`` for a single day.

    Returns:
        One ``datetime`` per day, preserving the time-of-day of ``start_date``.
    """
    effective_end = end_date or start_date
    num_days = (effective_end - start_date).days + 1
    return [start_date + timedelta(days=i) for i in range(num_days)]


def _submit_days_as_deployment_runs(
    deployment_name: str,
    days: list[date],
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
            f"Submitted {deployment_name} run for {day} as flow run "
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


def _configuration_for_deployment(
    configuration: "PrefectScriptedL2CalibrationConfig | SetQualityAndNaNConfig | GradiometryConfig",
) -> "dict | SetQualityAndNaNConfig | GradiometryConfig":
    """Prepare configuration for passing to ``run_deployment``.

    For ``PrefectScriptedL2CalibrationConfig``, any block stored in ``matlab_repo``
    is replaced with a ``{"$ref": {"block_document_id": "..."}}`` reference so that
    the child flow run re-loads the block from the block store (including its
    credentials) rather than receiving a plain-dict serialisation that drops them.

    Non-``PrefectScriptedL2CalibrationConfig`` configurations are returned unchanged.

    Args:
        configuration: The calibration configuration to prepare.

    Returns:
        A deployment-safe representation of the configuration.
    """
    if not isinstance(configuration, PrefectScriptedL2CalibrationConfig):
        return configuration

    matlab_repo = configuration.matlab_repo
    if isinstance(matlab_repo, (GitHubRepository, LocalFileSystem)):
        block_doc_id = getattr(matlab_repo, "_block_document_id", None)
        if block_doc_id:
            config_dict = configuration.model_dump()
            config_dict["matlab_repo"] = {
                "$ref": {"block_document_id": str(block_doc_id)}
            }
            return config_dict

    return configuration


def _resolve_matlab_repo_path(
    matlab_repo: "LocalFileSystem | GitHubRepository | str | None",
    work_folder: Path,
) -> Path | None:
    """Resolve the ``matlab_repo`` argument to a local path to the MATLAB code.

    ``matlab_repo`` may be a block name (str), a LocalFileSystem block (local path),
    a GitHubRepository block (pulled into a subfolder of the work folder named after
    the repo), or None. Raises if a provided repo cannot be found or pulled.
    """
    if not matlab_repo and Path(DEFAULT_MATLAB_REPO_PATH).exists():
        return Path(DEFAULT_MATLAB_REPO_PATH)

    block: LocalFileSystem | GitHubRepository | None = None
    if isinstance(matlab_repo, str):
        block = _load_matlab_repo_block(matlab_repo)
        if block is None:
            repo_path = Path(matlab_repo)
            if not repo_path.is_dir():
                raise ValueError(
                    f"Could not load a MATLAB repository block named '{matlab_repo}' or resolve it as a local path."
                )
            logger.info(f"Using local MATLAB calibration repository at {repo_path}")
            return repo_path
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


def _run_calibration(
    configuration,
    start_date: date,
    end_date: date | None,
    mode,
    sensor,
    save_mode,
    metakernel,
    layer_data_format: LayerDataFormat = LayerDataFormat.PARQUET,
) -> list[Path]:
    layer_file_version_number_override = None
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
        layer_file_version_number_override = (
            (
                configuration.layer_version_number_override.major,
                configuration.layer_version_number_override.minor,
            )
            if configuration.layer_version_number_override
            else None
        )

    output_file_paths: list[Path] = calibrate(
        start_date=datetime.combine(start_date, datetime.min.time()).replace(
            tzinfo=None
        )
        if start_date
        else None,
        end_date=datetime.combine(end_date, datetime.min.time()).replace(tzinfo=None)
        if end_date
        else None,
        method=configuration.get_method(),
        mode=mode,
        sensor=sensor,
        configuration=configuration.model_dump_json() if configuration else None,
        save_mode=save_mode,
        metakernel=metakernel,
        cleanup_temp_files_after_run=configuration.cleanup_temp_files_after_run,
        version_number_override=layer_file_version_number_override,
        layer_data_format=layer_data_format,
    )

    return output_file_paths
