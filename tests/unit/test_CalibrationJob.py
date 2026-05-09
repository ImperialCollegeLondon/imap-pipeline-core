"""Tests for CalibrationJob base class."""

from pathlib import Path
from unittest.mock import MagicMock

from mag_toolkit.calibration.CalibrationJobParameters import CalibrationJobParameters
from mag_toolkit.calibration.calibrators.CalibrationJob import CalibrationJob


def _make_concrete_job(work_folder=None):
    class ConcreteJob(CalibrationJob):
        def _get_path_handlers(self, params):
            return {}

        def run_calibration(self, cal_handler, config):
            return (Path("/out.csv"), Path("/cal.csv"))

    params = MagicMock(spec=CalibrationJobParameters)
    return ConcreteJob(params, work_folder or Path("/tmp"))


class TestCalibrationJobBase:
    def test_set_file_sets_required_file(self):
        job = _make_concrete_job()
        job.required_files["science"] = None
        job.set_file("science", Path("/data/science.cdf"))
        assert job.required_files["science"] == Path("/data/science.cdf")

    def test_set_file_logs_warning_when_file_already_set(self):
        job = _make_concrete_job()
        job.required_files["science"] = Path("/existing.cdf")
        job.set_file("science", Path("/new.cdf"))
        assert job.required_files["science"] == Path("/existing.cdf")

    def test_check_for_required_files_returns_true_when_all_present(self):
        job = _make_concrete_job()
        job.required_files["file1"] = Path("/data/file1.cdf")
        assert job._check_for_required_files() is True

    def test_check_for_required_files_returns_false_when_file_missing(self):
        job = _make_concrete_job()
        job.required_files["file1"] = None
        assert job._check_for_required_files() is False

    def test_check_for_required_data_store_returns_true_when_data_store_set(self):
        job = _make_concrete_job()
        job.data_store = Path("/datastore")
        assert job._check_for_required_data_store() is True

    def test_check_for_required_data_store_returns_false_when_data_store_none(self):
        job = _make_concrete_job()
        assert job.data_store is None
        assert job._check_for_required_data_store() is False

    def test_check_environment_returns_false_when_files_missing(self):
        job = _make_concrete_job()
        job.required_files["file1"] = None
        assert job._check_environment_is_setup() is False

    def test_check_environment_returns_false_when_data_store_missing(self):
        job = _make_concrete_job()
        assert job._check_environment_is_setup() is False

    def test_check_environment_returns_true_when_everything_set(self):
        job = _make_concrete_job()
        job.data_store = Path("/datastore")
        assert job._check_environment_is_setup() is True

    def test_setup_datastore_sets_data_store(self):
        job = _make_concrete_job()
        job.setup_datastore(Path("/datastore"))
        assert job.data_store == Path("/datastore")

    def test_setup_datastore_skips_when_not_needed(self):
        class NoDataStoreJob(CalibrationJob):
            def _get_path_handlers(self, params):
                return {}

            def run_calibration(self, cal_handler, config):
                return (Path("/out.csv"), Path("/cal.csv"))

            def needs_data_store(self):
                return False

        params = MagicMock(spec=CalibrationJobParameters)
        job = NoDataStoreJob(params, Path("/tmp"))
        job.setup_datastore(Path("/datastore"))
        assert job.data_store is None
