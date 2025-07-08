import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

from imap_mag.util import ScienceMode
from mag_toolkit.calibration import Sensor

logger = logging.getLogger(__name__)


class Calibrator(ABC):
    data_store: Path | None = None

    date: datetime
    mode: ScienceMode = ScienceMode.Normal  # Default mode
    sensor: Sensor = Sensor.MAGO  # Default sensor

    def __init__(self):
        self.required_files: dict = {}

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
    def _get_path_handlers(self, date, mode, sensor) -> dict:
        """
        Get the path handlers for the files needed for calibration.
        :param date: The date for which to get the handlers.
        :param mode: The science mode.
        :param sensor: The sensor type.
        :return: A dictionary of path handlers."""

    def _check_for_required_files(self):
        """
        Check if all required files are present.
        :return: True if all required files are present, False otherwise."""
        for file_key in self.required_files:
            if self.required_files[file_key] is None:
                logger.error(f"Required file {file_key} is missing.")
                return False
        return True

    def get_handlers_of_files_needed_for_calibration(self):
        """
        Get the handlers of files needed for gradiometry calibration.
        :param date: The date for which to get the handlers.
        :param mode: The science mode.
        :param sensor: The sensor type.
        :return: A tuple containing lists of science and other path handlers."""

        return self._get_path_handlers(self.date, self.mode, self.sensor)

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
        self.data_store = datastore

    @abstractmethod
    def run_calibration(self, calfile, config=None) -> Path:
        """Calibration that generates a calibration layer."""
