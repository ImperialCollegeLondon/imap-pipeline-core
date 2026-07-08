import json
import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from imap_mag.config import AppSettings
from imap_mag.config.CalibrationConfig import (
    CalibrationConfig,
    ScriptedL2CalibrationConfig,
)
from imap_mag.io.file import CalibrationLayerPathHandler
from imap_mag.io.file.SPICEPathHandler import SPICEPathHandler
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import CalibrationJobParameters
from mag_toolkit.calibration.CalibrationDefinitions import (
    CalibrationMethod,
    DatastoreAccessMode,
)
from mag_toolkit.calibration.MatlabWrapper import call_matlab
from mag_toolkit.calibration.SparseDatastoreBuilder import SparseDatastoreBuilder

from .CalibrationJob import CalibrationJob

logger = logging.getLogger(__name__)

# Name of the dynamically-generated MATLAB user/env config file written to the
# work folder before each run and cleaned up afterwards.
USER_CONFIG_FILENAME = "imap_mag_scripted_l2_file_path_config.json"

# Folder (inside the work folder) that holds the sparse datastore copy when using
# DatastoreAccessMode.LOCAL_WORK_FOLDER_COPY.
SPARSE_DATASTORE_FOLDER_NAME = "sparse_datastore"

# Relative path (within the MATLAB repo) of the script we invoke. Used to fail fast
# if the acquired repo does not actually contain the expected entry point.
MATLAB_SCRIPT_RELATIVE_PATH = (
    Path("+calibration") / "+scripts" / "calibrate_l2_offsets.m"
)


class ScriptedL2CalibrationJob(CalibrationJob):
    """Calibration job that runs the external MATLAB ``calibrate_l2_offsets`` pipeline.

    Unlike the other calibrators, the MATLAB script reads its L1 inputs from a
    datastore (``sharepoint_flight_data``) and fixes its own output filenames
    (``imap_mag_manual-{mode}-layer_{date}_v{VVV}.json``). This job therefore
    fetches no science files itself; instead it:

    * generates a MATLAB user/env config file (in the work folder) mapping the
      datastore + work-folder output paths,
    * resolves (or generates) the SPICE metakernel,
    * optionally builds a sparse local copy of the datastore in the work folder
      (see :class:`DatastoreAccessMode`),
    * invokes ``calibration.scripts.calibrate_l2_offsets`` from the root of the
      acquired MATLAB repository,
    * collects the produced layer JSON + CSV from the work folder.
    """

    def __init__(
        self,
        calibration_job_parameters: CalibrationJobParameters,
        app_settings: AppSettings,
        matlab_repo_path: Path,
        metakernel: Path | str | None = None,
        datastore_access_mode: DatastoreAccessMode = DatastoreAccessMode.READ_DIRECTLY,
    ):
        self.matlab_repo_path = Path(matlab_repo_path)
        if self.matlab_repo_path is None or not self.matlab_repo_path.is_dir():
            raise ValueError(
                "A valid matlab-repo-path (an existing directory) is required for the scripted-l2 calibration method."
            )
        matlab_script = self.matlab_repo_path / MATLAB_SCRIPT_RELATIVE_PATH
        if not matlab_script.is_file():
            raise FileNotFoundError(
                f"Expected MATLAB script not found at {matlab_script}; the acquired "
                "repository does not contain calibrate_l2_offsets."
            )

        work_folder = app_settings.setup_work_folder_for_command(
            app_settings.calibrate,
            name_context=self._work_folder_context(calibration_job_parameters),
        )
        super().__init__(calibration_job_parameters, work_folder)
        self.name = CalibrationMethod.SCRIPTED_L2_CALIBRATION
        self.app_settings = app_settings
        self.metakernel = metakernel
        self.datastore_access_mode = datastore_access_mode

    @staticmethod
    def _work_folder_context(
        calibration_job_parameters: CalibrationJobParameters,
    ) -> dict[str, str]:
        return {
            "date": calibration_job_parameters.date.strftime("%Y%m%d"),
            "mode": calibration_job_parameters.mode.value,
            "sensor": calibration_job_parameters.sensor.value,
        }

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

        date = self.calibration_job_parameters.date
        mode = self.calibration_job_parameters.mode
        output_data_version = cal_handler.version

        metakernel_filename = self._resolve_metakernel(date)

        # Decide which datastore MATLAB will read from, building a sparse local copy
        # in the work folder if requested.
        sparse_datastore: Path | None = None
        if self.datastore_access_mode == DatastoreAccessMode.LOCAL_WORK_FOLDER_COPY:
            sparse_datastore = self._build_sparse_datastore(
                date, mode, metakernel_filename, config.calibration_matrix_version
            )
            matlab_datastore = sparse_datastore
        else:
            matlab_datastore = self.data_store

        user_config_path = self._write_user_config(matlab_datastore)

        try:
            command = self._build_matlab_command(
                date=date,
                calibration_matrix_version=config.calibration_matrix_version,
                metakernel_filename=metakernel_filename,
                output_data_version=output_data_version,
                input_json_file=config.input_json_file,
                user_config_path=user_config_path,
                matlab_mode=mode.value,
            )

            call_matlab(
                command,
                cwd=self.matlab_repo_path,
                include_project_paths=False,
                timeout=self._timeout_seconds(mode),
            )
        finally:
            if user_config_path.exists():
                logger.info(
                    f"Cleaning up generated MATLAB user config file {user_config_path}"
                )
                user_config_path.unlink()
            if sparse_datastore is not None and sparse_datastore.exists():
                logger.info(f"Cleaning up sparse datastore at {sparse_datastore}")
                shutil.rmtree(sparse_datastore, ignore_errors=True)

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

    def _timeout_seconds(self, mode: ScienceMode) -> int:
        """MATLAB timeout for a single day of the given mode.

        Config holds a per-day timeout for each mode; a single ``calibrate_l2_offsets``
        invocation processes one day, so a multi-day range (multiple invocations)
        naturally gets a proportionally larger total budget.
        """
        calibrate_config = self.app_settings.calibrate
        if mode == ScienceMode.Burst:
            return calibrate_config.scripted_l2_timeout_seconds_per_day_burst
        return calibrate_config.scripted_l2_timeout_seconds_per_day_norm

    def _build_sparse_datastore(
        self,
        date: datetime,
        mode: ScienceMode,
        metakernel_filename: str,
        matrix_version: int,
    ) -> Path:
        assert self.data_store is not None
        builder = SparseDatastoreBuilder(
            source_datastore=self.data_store,
            config=self.app_settings.calibrate.sparse_datastore,
            disk_usage_threshold=self.app_settings.disk_usage_threshold,
        )
        target_root = self.work_folder / SPARSE_DATASTORE_FOLDER_NAME
        if target_root.exists():
            shutil.rmtree(target_root, ignore_errors=True)
        return builder.build(
            target_root, [date], mode, metakernel_filename, matrix_version
        )

    def _resolve_metakernel(self, date: datetime) -> str:
        """Return the metakernel filename to pass to MATLAB.

        MATLAB looks the metakernel up under ``{datastore}/spice/mk/``, so only the
        bare filename is passed. If a metakernel was provided we verify it exists
        there; otherwise we generate one (like the apply flow does) and publish it
        to the datastore so MATLAB can find it.
        """
        assert self.data_store is not None

        if self.metakernel is not None:
            filename = Path(self.metakernel).name
            mk_path = SPICEPathHandler.get_metakernel_path(self.data_store, filename)
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
            file_types=self.app_settings.metakernel_file_types,
            verify=False,
            publish_to_datastore=True,
        )
        generated_path = Path(
            generated[0] if isinstance(generated, list) else generated
        )
        filename = generated_path.name

        mk_path = SPICEPathHandler.get_metakernel_path(self.data_store, filename)
        if not mk_path.exists():
            raise FileNotFoundError(
                f"Generated metakernel '{filename}' was not published to {mk_path}."
            )
        logger.info(f"Generated and published metakernel {filename} to {mk_path}")
        return filename

    def _write_user_config(self, matlab_datastore: Path) -> Path:
        """Write the MATLAB user/env file-path config JSON to the work folder.

        ``sharepoint_flight_data`` and ``spice_metakernal_root`` point at the
        datastore root MATLAB should read (the real datastore, or the sparse copy),
        while the three output folders map to the work folder so MATLAB writes there
        rather than into the datastore.
        """
        datastore_path = Path(matlab_datastore).resolve()
        work_folder_path = str(self.work_folder.resolve())

        config = {
            "sharepoint_flight_data": str(datastore_path),
            "spice_metakernal_root": str(datastore_path),
            "l2_pre_calibration_outputs": work_folder_path,
            "report_folder": str(datastore_path / "calibration" / "reports"),
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
            "publish_to_sharepoint=false, display_plots=false,spice_transform_and_write=false)"
        )
