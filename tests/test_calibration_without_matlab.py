from datetime import datetime
from pathlib import Path

from imap_mag.cli.calibrate import calibrate, gradiometry
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import CalibrationMethod, Sensor
from tests.util.miscellaneous import (
    TEST_DATA,
    copy_test_file,
    create_test_file,
)


def _generate_layer_json(data_file_name):
    json_contents = """
    {
        "id": "",
        "metadata": {
            "comment": "",
            "creation_timestamp": "2025-03-28T11:22:20",
            "data_filename": "FILENAME_PLACEHOLDER",
            "dependencies": [],
            "science": []
        },
        "method": "noop",
        "mission": "IMAP",
        "sensor": "MAGo",
        "validity": {
            "end": "2025-10-17T02:11:59.021309",
            "start": "2025-10-17T02:11:51.521"
        },
        "value_type": "vector",
        "version": 0.0
    }
    """
    return json_contents.replace("FILENAME_PLACEHOLDER", data_file_name)


def test_empty_calibrator_makes_correct_matlab_call(
    monkeypatch,
    temp_datastore,
    dynamic_work_folder,
):
    copy_test_file(
        TEST_DATA / "imap_mag_l1c_norm-mago-four-vectors-four-ranges_20251017_v000.cdf",
        temp_datastore / "science/mag/l1c/2025/10",
        "imap_mag_l1c_norm-mago_20251017_v000.cdf",
    )

    def mock_call_matlab(command):
        assert command.startswith("calibration.wrappers.run_empty_calibrator")
        create_test_file(
            Path(dynamic_work_folder) / "imap_mag_noop-norm-layer_20251017_v001.json",
            _generate_layer_json("imap_mag_noop-norm-layer-data_20251017_v001.csv"),
        )
        create_test_file(
            Path(dynamic_work_folder)
            / "imap_mag_noop-norm-layer-data_20251017_v001.csv"
        )

    monkeypatch.setattr(
        "mag_toolkit.calibration.calibrators.EmptyCalibration.call_matlab",
        mock_call_matlab,
    )

    calibrate(
        start_date=datetime(2025, 10, 17),
        sensor=Sensor.MAGO,
        mode=ScienceMode.Normal,
        method=CalibrationMethod.NOOP,
    )

    assert (
        temp_datastore
        / "calibration/layers/2025/10/imap_mag_noop-norm-layer_20251017_v001.json"
    ).exists()
    assert (
        temp_datastore
        / "calibration/layers/2025/10/imap_mag_noop-norm-layer-data_20251017_v001.csv"
    ).exists()


def test_gradiometer_calibrator_makes_correct_matlab_call(
    monkeypatch,
    temp_datastore,
    dynamic_work_folder,
):
    def mock_call_matlab(command):
        assert command.startswith("calibration.wrappers.run_gradiometry")
        create_test_file(
            dynamic_work_folder / "imap_mag_gradiometer-norm-layer_20260930_v001.json",
            _generate_layer_json(
                "imap_mag_gradiometer-norm-layer-data_20260930_v001.csv"
            ),
        )
        create_test_file(
            dynamic_work_folder
            / "imap_mag_gradiometer-norm-layer-data_20260930_v001.csv"
        )

    monkeypatch.setattr(
        "mag_toolkit.calibration.calibrators.GradiometerCalibration.call_matlab",
        mock_call_matlab,
    )

    gradiometry(
        start_date=datetime(2026, 9, 30),
        mode=ScienceMode.Normal,
        kappa=0.25,
        sc_interference_threshold=10.0,
    )

    layer_metadata = (
        temp_datastore
        / "calibration/layers/2026/09/imap_mag_gradiometer-norm-layer_20260930_v001.json"
    )
    assert layer_metadata.exists()

    layer_data = (
        temp_datastore
        / "calibration/layers/2026/09/imap_mag_gradiometer-norm-layer-data_20260930_v001.csv"
    )
    assert layer_data.exists()


def test_gradiometer_calibrator_finds_next_viable_version(
    monkeypatch,
    temp_datastore,
    dynamic_work_folder,
):
    """Calibration always produces v001 output; datastore bumps to v002 when v001 already exists."""

    def mock_call_matlab(command):
        assert (
            command
            == f'calibration.wrappers.run_gradiometry("2026-09-30T00:00:00", "{dynamic_work_folder}/imap_mag_l1c_norm-mago_20260930_v001.cdf", "{dynamic_work_folder}/imap_mag_l1c_norm-magi_20260930_v001.cdf", "{dynamic_work_folder}/imap_mag_gradiometer-norm-layer_20260930_v001.json", "{dynamic_work_folder}/imap_mag_gradiometer-norm-layer-data_20260930_v001.csv", "{temp_datastore}", "0.25", "10.0")'
        )
        create_test_file(
            Path(dynamic_work_folder)
            / "imap_mag_gradiometer-norm-layer_20260930_v001.json",
            _generate_layer_json(
                "imap_mag_gradiometer-norm-layer-data_20260930_v001.csv"
            ),
        )
        create_test_file(
            Path(dynamic_work_folder)
            / "imap_mag_gradiometer-norm-layer-data_20260930_v001.csv",
            "col\n1\n2\n",
        )

    monkeypatch.setattr(
        "mag_toolkit.calibration.calibrators.GradiometerCalibration.call_matlab",
        mock_call_matlab,
    )

    # Pre-populate datastore with a different v001 (empty content differs from new)
    existing_layer = (
        temp_datastore
        / "calibration/layers/2026/09/imap_mag_gradiometer-norm-layer_20260930_v001.json"
    )
    existing_layer.parent.mkdir(parents=True, exist_ok=True)
    existing_layer.touch()

    gradiometry(
        start_date=datetime(2026, 9, 30),
        mode=ScienceMode.Normal,
        kappa=0.25,
        sc_interference_threshold=10.0,
    )

    # Datastore must have versioned up to v002
    assert (
        temp_datastore
        / "calibration/layers/2026/09/imap_mag_gradiometer-norm-layer_20260930_v002.json"
    ).exists()
    assert (
        temp_datastore
        / "calibration/layers/2026/09/imap_mag_gradiometer-norm-layer-data_20260930_v002.csv"
    ).exists()


def test_calibration_layer_versioned_together_when_only_json_exists(
    monkeypatch,
    temp_datastore,
    dynamic_work_folder,
):
    """When only a JSON layer exists at v001 (no CSV), both JSON and CSV
    output files must be versioned as v002 — not JSON=v002, CSV=v001.
    Calibration always produces v001; the datastore bumps both to v002."""

    def mock_call_matlab(command):
        # Calibration always emits v001 — versioning is the datastore's job
        create_test_file(
            Path(dynamic_work_folder)
            / "imap_mag_gradiometer-norm-layer_20260930_v001.json",
            _generate_layer_json(
                "imap_mag_gradiometer-norm-layer-data_20260930_v001.csv"
            ),
        )
        create_test_file(
            Path(dynamic_work_folder)
            / "imap_mag_gradiometer-norm-layer-data_20260930_v001.csv",
            "col\n1\n2\n",
        )

    monkeypatch.setattr(
        "mag_toolkit.calibration.calibrators.GradiometerCalibration.call_matlab",
        mock_call_matlab,
    )

    # Pre-populate the datastore with only a JSON at v001 (no matching CSV)
    layers_dir = temp_datastore / "calibration/layers/2026/09"
    layers_dir.mkdir(parents=True, exist_ok=True)
    (layers_dir / "imap_mag_gradiometer-norm-layer_20260930_v001.json").touch()

    gradiometry(
        start_date=datetime(2026, 9, 30),
        mode=ScienceMode.Normal,
        kappa=0.25,
        sc_interference_threshold=10.0,
    )

    # Both files must share version v002
    assert (
        layers_dir / "imap_mag_gradiometer-norm-layer_20260930_v002.json"
    ).exists(), "JSON layer must be v002"
    assert (
        layers_dir / "imap_mag_gradiometer-norm-layer-data_20260930_v002.csv"
    ).exists(), "CSV data file must also be v002, not v001"


def test_calibration_layer_versioned_together_when_only_csv_exists(
    monkeypatch,
    temp_datastore,
    dynamic_work_folder,
):
    """When only a CSV layer-data exists at v001 (no JSON), both JSON and CSV
    output files must be versioned as v002 — not JSON=v001, CSV=v002.
    The sibling-check in DatastoreFileManager ensures the JSON version is
    also bumped when the companion CSV slot is already occupied."""

    def mock_call_matlab(command):
        # Calibration always emits v001 — versioning is the datastore's job
        create_test_file(
            Path(dynamic_work_folder)
            / "imap_mag_gradiometer-norm-layer_20260930_v001.json",
            _generate_layer_json(
                "imap_mag_gradiometer-norm-layer-data_20260930_v001.csv"
            ),
        )
        create_test_file(
            Path(dynamic_work_folder)
            / "imap_mag_gradiometer-norm-layer-data_20260930_v001.csv",
            "col\n1\n2\n",
        )

    monkeypatch.setattr(
        "mag_toolkit.calibration.calibrators.GradiometerCalibration.call_matlab",
        mock_call_matlab,
    )

    # Pre-populate the datastore with only a CSV at v001 (no matching JSON).
    # Its content differs from the new CSV so the sibling check must block v001.
    layers_dir = temp_datastore / "calibration/layers/2026/09"
    layers_dir.mkdir(parents=True, exist_ok=True)
    (layers_dir / "imap_mag_gradiometer-norm-layer-data_20260930_v001.csv").write_text(
        "col\nold\n"
    )

    gradiometry(
        start_date=datetime(2026, 9, 30),
        mode=ScienceMode.Normal,
        kappa=0.25,
        sc_interference_threshold=10.0,
    )

    # Both files must share version v002
    assert (
        layers_dir / "imap_mag_gradiometer-norm-layer_20260930_v002.json"
    ).exists(), "JSON layer must be v002"
    assert (
        layers_dir / "imap_mag_gradiometer-norm-layer-data_20260930_v002.csv"
    ).exists(), "CSV data file must also be v002, not v001"
