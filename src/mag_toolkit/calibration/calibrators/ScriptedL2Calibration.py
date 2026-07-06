import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

from imap_mag.config.CalibrationConfig import (
    CalibrationConfig,
    ScriptedL2CalibrationConfig,
)
from imap_mag.io.file import CalibrationLayerPathHandler
from mag_toolkit.calibration import CalibrationJobParameters
from mag_toolkit.calibration.CalibrationDefinitions import CalibrationMethod
from mag_toolkit.calibration.MatlabWrapper import call_matlab

from .CalibrationJob import CalibrationJob

logger = logging.getLogger(__name__)

# A single day of the MATLAB L2 calibration takes a few minutes; allow plenty of
# headroom so slow days (e.g. cold MATLAB start plus SPICE furnishing) do not time out.
SCRIPTED_L2_MATLAB_TIMEOUT_SECONDS = 60 * 30

# Name of the dynamically-generated MATLAB user/env config file written to the work
# folder before each run and cleaned up afterwards.
USER_CONFIG_FILENAME = "imap_mag_scripted_l2_file_path_config.json"

# SPICE kernel types included when generating a metakernel on demand. Mirrors the
# set used by CalibrationApplicator so generated metakernels are consistent.
_METAKERNEL_FILE_TYPES = [
    "leapseconds",
    "planetary_constants",
    "science_frames",
    "imap_frames",
    "spacecraft_clock",
    "attitude_history",
    "pointing_attitude",
    "planetary_ephemeris",
    "ephemeris_reconstructed",
]


class ScriptedL2CalibrationJob(CalibrationJob):
    """Calibration job that runs the external MATLAB ``calibrate_l2_offsets`` pipeline.

    Unlike the other calibrators, the MATLAB script reads its L1 inputs directly
    from the datastore (``sharepoint_flight_data``) and fixes its own output
    filenames (``imap_mag_manual-{mode}-layer_{date}_v{VVV}.json``). This job
    therefore fetches no science files itself; instead it:

    * generates a MATLAB user/env config file mapping datastore + work-folder paths,
    * resolves (or generates) the SPICE metakernel,
    * invokes ``calibration.scripts.calibrate_l2_offsets`` from the root of the
      acquired MATLAB repository,
    * collects the produced layer JSON + CSV from the work folder.
    """

    def __init__(
        self,
        calibration_job_parameters: CalibrationJobParameters,
        work_folder: Path,
        matlab_repo_path: Path,
        metakernel: Path | str | None = None,
    ):
        super().__init__(calibration_job_parameters, work_folder)
        self.name = CalibrationMethod.SCRIPTED_L2_CALIBRATION

        if matlab_repo_path is None:
            raise ValueError(
                "A MATLAB calibration repository path is required for scripted L2 calibration."
            )
        self.matlab_repo_path = Path(matlab_repo_path)
        self.metakernel = metakernel

    def _get_path_handlers(self, calibration_job_parameters: CalibrationJobParameters):
        # The MATLAB script reads L1 science directly from the datastore, so there
        # are no files to fetch into the work folder for this job.
        return {}

    def run_calibration(
        self, cal_handler: CalibrationLayerPathHandler, config: CalibrationConfig
    ) -> tuple[Path, Path]:
        if not isinstance(config, ScriptedL2CalibrationConfig):
            raise TypeError(
                "ScriptedL2CalibrationJob requires a ScriptedL2CalibrationConfig, "
                f"got {type(config).__name__}."
            )

        if not self._check_environment_is_setup():
            raise FileNotFoundError(
                "Environment has not been correctly set up for calibration."
            )

        if not self.matlab_repo_path.is_dir():
            raise FileNotFoundError(
                f"MATLAB calibration repository not found at {self.matlab_repo_path}."
            )

        date = self.calibration_job_parameters.date
        matlab_mode = self.calibration_job_parameters.mode.value  # "norm" / "burst"
        output_data_version = cal_handler.version

        metakernel_filename = self._resolve_metakernel(date)
        user_config_path = self._write_user_config()

        try:
            command = self._build_matlab_command(
                date=date,
                calibration_matrix_version=config.calibration_matrix_version,
                metakernel_filename=metakernel_filename,
                output_data_version=output_data_version,
                input_json_file=config.input_json_file,
                user_config_path=user_config_path,
                matlab_mode=matlab_mode,
            )

            call_matlab(
                command,
                cwd=self.matlab_repo_path,
                unset_display=True,
                include_project_paths=False,
                timeout=SCRIPTED_L2_MATLAB_TIMEOUT_SECONDS,
            )
        finally:
            if user_config_path.exists():
                logger.info(
                    f"Cleaning up generated MATLAB user config file {user_config_path}"
                )
                user_config_path.unlink()

        calfile = self.work_folder / cal_handler.get_filename()
        datafile = (
            self.work_folder / cal_handler.get_equivalent_data_handler().get_filename()
        )

        if not calfile.exists():
            raise FileNotFoundError(
                f"Calibration layer file {calfile} was not created by the MATLAB calibration."
            )
        if not datafile.exists():
            raise FileNotFoundError(
                f"Calibration data file {datafile} was not created by the MATLAB calibration."
            )

        return calfile, datafile

    def _resolve_metakernel(self, date: datetime) -> str:
        """Return the metakernel filename to pass to MATLAB.

        MATLAB looks the metakernel up under ``{sharepoint_flight_data}/spice/mk/``,
        so only the bare filename is passed. If a metakernel was provided we verify
        it exists there; otherwise we generate one (like the apply flow does) and
        publish it to the datastore so MATLAB can find it.
        """
        assert self.data_store is not None
        mk_dir = self.data_store / "spice" / "mk"

        if self.metakernel is not None:
            filename = Path(self.metakernel).name
            mk_path = mk_dir / filename
            if not mk_path.exists():
                raise FileNotFoundError(
                    f"Metakernel '{filename}' not found at {mk_path}. "
                    "It must exist in the spice/mk folder of the datastore."
                )
            logger.info(f"Using provided metakernel {filename} from {mk_path}")
            return filename

        logger.info(
            "No metakernel provided; generating one for the scripted L2 calibration."
        )
        # Imported lazily to avoid a heavy import chain (spiceypy etc.) at module load.
        from imap_mag.cli.fetch.spice import generate_spice_metakernel

        generated = generate_spice_metakernel(
            start_time=date + timedelta(hours=-1),
            end_time=date + timedelta(days=1, hours=1),
            file_types=_METAKERNEL_FILE_TYPES,
            verify=False,
            publish_to_datastore=True,
        )
        generated_path = Path(
            generated[0] if isinstance(generated, list) else generated
        )
        filename = generated_path.name

        mk_path = mk_dir / filename
        if not mk_path.exists():
            raise FileNotFoundError(
                f"Generated metakernel '{filename}' was not published to {mk_path}."
            )
        logger.info(f"Generated and published metakernel {filename} to {mk_path}")
        return filename

    def _write_user_config(self) -> Path:
        """Write the MATLAB user/env file-path config JSON to the work folder.

        ``sharepoint_flight_data`` and ``spice_metakernal_root`` point at the
        datastore root (the folder that contains ``spice/``), while the three
        output folders map to the work folder so MATLAB writes there rather than
        into the datastore.
        """
        assert self.data_store is not None

        datastore_path = str(self.data_store.resolve())
        work_folder_path = str(self.work_folder.resolve())

        config = {
            "sharepoint_flight_data": datastore_path,
            "spice_metakernal_root": datastore_path,
            "l2_pre_calibration_outputs": work_folder_path,
            "report_folder": work_folder_path,
            "output_layers_folder": work_folder_path,
        }

        user_config_path = self.work_folder / USER_CONFIG_FILENAME
        with open(user_config_path, "w") as fid:
            json.dump(config, fid, indent=4)

        logger.info(
            f"Wrote MATLAB user/env config to {user_config_path}:\n"
            f"{json.dumps(config, indent=2)}"
        )
        return user_config_path

    def _build_matlab_command(
        self,
        date: datetime,
        calibration_matrix_version: int,
        metakernel_filename: str,
        output_data_version: int,
        input_json_file: str,
        user_config_path: Path,
        matlab_mode: str,
    ) -> str:
        """Build the ``calibrate_l2_offsets`` MATLAB command for a single day."""
        date_expr = f"datetime({date.year},{date.month},{date.day})"

        return (
            "calibration.scripts.calibrate_l2_offsets("
            f"{date_expr}, {date_expr}, "
            f"{calibration_matrix_version}, "
            f'"{metakernel_filename}", '
            f"{output_data_version}, "
            f'"{input_json_file}", '
            f'"{user_config_path.resolve()!s}", '
            f'modes=["{matlab_mode}"], '
            "publish_to_sharepoint=false, display_plots=false)"
        )
