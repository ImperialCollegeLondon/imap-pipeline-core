import logging
from abc import ABC, abstractmethod
from pathlib import Path

from imap_mag.cli.cliUtils import fetch_file_for_work
from imap_mag.config.CalibrationConfig import CalibrationConfig
from imap_mag.io import FileFinder
from imap_mag.io.file import CalibrationLayerPathHandler
from mag_toolkit.calibration.CalibrationJobParameters import CalibrationJobParameters

logger = logging.getLogger(__name__)


class CalibrationJob(ABC):
    data_store: Path | None = None

    calibration_job_parameters: CalibrationJobParameters
    work_folder: Path

    def __init__(
        self, calibration_job_parameters: CalibrationJobParameters, work_folder: Path
    ):
        self.required_files: dict = dict()
        self.calibration_job_parameters = calibration_job_parameters
        self.work_folder = work_folder

    def setup_calibration_files(
        self,
        datastore_finder: FileFinder,
    ):
        path_handlers = self._get_path_handlers(self.calibration_job_parameters)

        for key in path_handlers:
            path_handler = path_handlers[key]
            input_file = datastore_finder.find_latest_version_by_handler(
                path_handler, throw_if_not_found=True
            )
            work_file = fetch_file_for_work(
                input_file, self.work_folder, throw_if_not_found=True
            )
            self.set_file(key, work_file)

    def set_file(self, file_key, filepath):
        """
        Add a file to the calibrator.
        :param file: The path to the file to be added.
        """
        if not self.required_files[file_key]:
            self.required_files[file_key] = filepath
        else:
            logger.warning(f"File {file_key} already exists in required files.")

    @abstractmethod
    def _get_path_handlers(
        self, calibration_job_parameters: CalibrationJobParameters
    ) -> dict:
        """
        Get the path handlers for the files needed for calibration.
        :param date: The date for which to get the handlers.
        :param mode: The science mode.
        :param sensor: The sensor type.
        :return: A dictionary of path handlers."""

    def set_layer_to_next_viable_version(
        self,
        datastore_finder: FileFinder,
        layer_handler: CalibrationLayerPathHandler,
    ):
        """
        Get the next viable version for a calibration layer.

        Checks both the JSON metadata file and the CSV data file so that the pair
        is always versioned together — whichever file has the higher existing version
        determines the base for the next version number.

        :return: Calibration layer handler for next viable version.
        """

        def _version_of(path: Path | None) -> int:
            if path is None:
                return 0
            h = CalibrationLayerPathHandler.from_filename(path)
            return h.version if h is not None else 0

        latest_json_file: Path | None = datastore_finder.find_latest_version_by_handler(
            layer_handler, throw_if_not_found=False
        )
        latest_csv_file: Path | None = datastore_finder.find_latest_version_by_handler(
            layer_handler.get_equivalent_data_handler(), throw_if_not_found=False
        )

        max_existing_version = max(
            _version_of(latest_json_file), _version_of(latest_csv_file)
        )
        if max_existing_version >= layer_handler.version:
            layer_handler.version = max_existing_version
            layer_handler.increase_sequence()

    def _check_environment_is_setup(self):
        if not self._check_for_required_files():
            logger.error("Required files are incomplete")
            return False

        if not self._check_for_required_data_store():
            logger.error("Data store needs to be set up before calibration can be run")
            return False
        return True

    def _check_for_required_data_store(self):
        if self.needs_data_store() and self.data_store is None:
            logger.error("Data store is not set up")
            return False
        return True

    def _check_for_required_files(self):
        """
        Check if all required files are present.
        :return: True if all required files are present, False otherwise."""
        for file_key in self.required_files:
            if self.required_files[file_key] is None:
                logger.error(f"Required file {file_key} is missing.")
                return False
        return True

    def needs_data_store(self):
        """
        Check if the calibrator needs a data store.
        :return: True, as most calibration requires a data store."""
        return True

    def setup_datastore(self, datastore: Path):
        """
        Setup the data store for the calibrator.
        :param datastore: The path to the data store.
        """
        if self.needs_data_store():
            self.data_store = datastore

    @abstractmethod
    def run_calibration(
        self, cal_handler: CalibrationLayerPathHandler, config: CalibrationConfig
    ) -> tuple[Path, Path]:
        """Calibration that generates a calibration layer."""
