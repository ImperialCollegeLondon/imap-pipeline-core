import tempfile
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from imap_mag.cli.apply import apply
from imap_mag.cli.calibrate import calibrate
from imap_mag.config import SaveMode
from imap_mag.config.CalibrationConfig import CalibrationConfig, SetQualityAndNaNConfig
from imap_mag.io.file.CalibrationLayerPathHandler import CalibrationLayerPathHandler
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import (
    CalibrationLayer,
    CalibrationMethod,
    Sensor,
    SetQualityAndNaNCalibrationJob,
)
from mag_toolkit.calibration.CalibrationDefinitions import CONSTANTS, ValueType
from mag_toolkit.calibration.CalibrationJobParameters import CalibrationJobParameters
from tests.util.miscellaneous import open_cdf


@pytest.fixture
def quality_csv(tmp_path):
    csv_content = (
        "start_date,end_date,quality_flag,quality_bitmask,nan_x,nan_y,nan_z\n"
        "2026-01-16T02:11:54,2026-01-16T02:11:57,1,3,True,False,False\n"
    )
    csv_path = tmp_path / "quality_input.csv"
    csv_path.write_text(csv_content)
    return csv_path


def test_calibration_job_creates_quality_flag_layer_file_json_and_csv_with_correct_contents(
    quality_csv, tmp_path
):
    """Verify the calibration job creates correct change-point rows."""
    params = CalibrationJobParameters(
        date=datetime(2026, 1, 16),
        mode=ScienceMode.Normal,
        sensor=Sensor.MAGO,
    )
    work_folder = tmp_path / "work"
    work_folder.mkdir()

    config = CalibrationConfig(
        set_quality_and_nan=SetQualityAndNaNConfig(csv_file=str(quality_csv))
    )

    cal_handler = CalibrationLayerPathHandler(
        descriptor=CalibrationMethod.SET_QUALITY_AND_NAN.short_name,
        content_date=datetime(2026, 1, 16),
    )

    job = SetQualityAndNaNCalibrationJob(params, work_folder)
    calfile, datafile = job.run_calibration(cal_handler, config)

    assert calfile.exists()
    assert datafile.exists()

    layer = CalibrationLayer.from_file(calfile, load_contents=True)
    assert layer.value_type == ValueType.BOUNDARY_CHANGES_ONLY
    assert layer.method == CalibrationMethod.SET_QUALITY_AND_NAN

    df = pd.read_csv(datafile, parse_dates=[CONSTANTS.CSV_VARS.EPOCH])
    assert len(df) == 2

    start_row = df.iloc[0]
    assert start_row[CONSTANTS.CSV_VARS.QUALITY_FLAG] == 1
    assert start_row[CONSTANTS.CSV_VARS.QUALITY_BITMASK] == 3
    assert np.isnan(start_row[CONSTANTS.CSV_VARS.OFFSET_X])
    assert start_row[CONSTANTS.CSV_VARS.OFFSET_Y] == 0.0
    assert start_row[CONSTANTS.CSV_VARS.OFFSET_Z] == 0.0

    end_row = df.iloc[1]
    # End row undoes the window: flag was 1 → -1 clears it; bitmask was 3 → -3 clears those bits
    assert end_row[CONSTANTS.CSV_VARS.QUALITY_FLAG] == -1
    assert end_row[CONSTANTS.CSV_VARS.QUALITY_BITMASK] == -3
    assert end_row[CONSTANTS.CSV_VARS.OFFSET_X] == 0.0
    assert end_row[CONSTANTS.CSV_VARS.OFFSET_Y] == 0.0
    assert end_row[CONSTANTS.CSV_VARS.OFFSET_Z] == 0.0


def test_run_calibration_writes_epoch_as_full_iso_datetime_when_clipped_to_day_boundary(
    tmp_path,
):
    csv_content = (
        "start_date,end_date,quality_flag,quality_bitmask,nan_x,nan_y,nan_z\n"
        "2026-01-15,2026-01-17,1,3,True,False,False\n"  # starts before calday
    )
    config = create_temporary_csv_config(csv_content)

    params = CalibrationJobParameters(
        date=datetime(2026, 1, 16), mode=ScienceMode.Normal, sensor=Sensor.MAGO
    )
    work_folder = tmp_path / "work"
    work_folder.mkdir()
    handler = CalibrationLayerPathHandler(
        descriptor=CalibrationMethod.SET_QUALITY_AND_NAN.short_name,
        content_date=datetime(2026, 1, 16),
    )
    job = SetQualityAndNaNCalibrationJob(params, work_folder)

    _, datafile = job.run_calibration(handler, config)

    raw_lines = datafile.read_text().splitlines()
    # Second line is the first data row; its epoch value is the first field
    first_epoch_str = raw_lines[1].split(",")[0]
    # Must contain a time component — a bare date like "2026-01-16" has no 'T'
    assert "2026-01-16T00:00:00" in first_epoch_str, (
        f"Epoch should be a full ISO datetime but got: {first_epoch_str!r}"
    )


def test_calibration_job_splits_across_days(tmp_path):
    """Verify a window spanning two days creates change points for each day."""
    csv_content = (
        "start_date,end_date,quality_flag,quality_bitmask,nan_x,nan_y,nan_z\n"
        "2026-01-16T20:00:00,2026-01-17T06:00:00,1,5,False,True,True\n"
    )
    config = create_temporary_csv_config(csv_content)
    work_folder = tmp_path / "work"
    work_folder.mkdir()

    # Day 1: 2026-01-16
    params_day1 = CalibrationJobParameters(
        date=datetime(2026, 1, 16), mode=ScienceMode.Normal, sensor=Sensor.MAGO
    )
    handler_day1 = CalibrationLayerPathHandler(
        descriptor=CalibrationMethod.SET_QUALITY_AND_NAN.short_name,
        content_date=datetime(2026, 1, 16),
    )
    job_day1 = SetQualityAndNaNCalibrationJob(params_day1, work_folder)
    _, datafile1 = job_day1.run_calibration(handler_day1, config)

    df1 = pd.read_csv(datafile1, parse_dates=[CONSTANTS.CSV_VARS.EPOCH])
    # Day 1: window starts at 20:00, no end within day -> 1 change point
    assert len(df1) == 1
    assert df1.iloc[0][CONSTANTS.CSV_VARS.EPOCH] == pd.Timestamp("2026-01-16T20:00:00")
    assert df1.iloc[0][CONSTANTS.CSV_VARS.QUALITY_FLAG] == 1
    assert df1.iloc[0][CONSTANTS.CSV_VARS.QUALITY_BITMASK] == 5

    # Day 2: 2026-01-17
    work_folder2 = tmp_path / "work2"
    work_folder2.mkdir()
    params_day2 = CalibrationJobParameters(
        date=datetime(2026, 1, 17), mode=ScienceMode.Normal, sensor=Sensor.MAGO
    )
    handler_day2 = CalibrationLayerPathHandler(
        descriptor=CalibrationMethod.SET_QUALITY_AND_NAN.short_name,
        content_date=datetime(2026, 1, 17),
    )
    job_day2 = SetQualityAndNaNCalibrationJob(params_day2, work_folder2)
    _, datafile2 = job_day2.run_calibration(handler_day2, config)

    df2 = pd.read_csv(datafile2, parse_dates=[CONSTANTS.CSV_VARS.EPOCH])
    # Day 2: window starts at 00:00 (clipped), ends at 06:00 -> 2 change points
    assert len(df2) == 2
    assert df2.iloc[0][CONSTANTS.CSV_VARS.EPOCH] == pd.Timestamp("2026-01-17T00:00:00")
    assert df2.iloc[0][CONSTANTS.CSV_VARS.QUALITY_FLAG] == 1
    assert df2.iloc[0][CONSTANTS.CSV_VARS.QUALITY_BITMASK] == 5
    assert df2.iloc[1][CONSTANTS.CSV_VARS.EPOCH] == pd.Timestamp("2026-01-17T06:00:00")
    # End row undoes the window: flag was 1 → -1; bitmask was 5 → -5
    assert df2.iloc[1][CONSTANTS.CSV_VARS.QUALITY_FLAG] == -1
    assert df2.iloc[1][CONSTANTS.CSV_VARS.QUALITY_BITMASK] == -5


def run_calibration_on_config_file(
    tmp_folder_path, csv_content, content_date=datetime(2026, 1, 16)
):
    config = create_temporary_csv_config(csv_content)

    work_folder = tmp_folder_path / "work"
    work_folder.mkdir()

    params_day1 = CalibrationJobParameters(
        date=content_date, mode=ScienceMode.Normal, sensor=Sensor.MAGO
    )
    handler_day1 = CalibrationLayerPathHandler(
        descriptor=CalibrationMethod.SET_QUALITY_AND_NAN.short_name,
        content_date=content_date,
    )
    job_day1 = SetQualityAndNaNCalibrationJob(params_day1, work_folder)
    json_file, datafile1 = job_day1.run_calibration(handler_day1, config)

    assert json_file.exists()
    assert datafile1.exists()

    layer_data_file = pd.read_csv(datafile1, parse_dates=[CONSTANTS.CSV_VARS.EPOCH])
    return layer_data_file


def test_run_calibration_starts_at_midnight_if_config_if_in_a_previous_day(
    tmp_path,
):
    csv_content = (
        "start_date,end_date,quality_flag,quality_bitmask,nan_x,nan_y,nan_z\n"
        "2026-01-15T20:00:00,2026-01-17T06:00:00,1,5,False,True,True\n"
    )
    df1 = run_calibration_on_config_file(
        tmp_path, csv_content, content_date=datetime(2026, 1, 16)
    )

    assert len(df1) == 1
    assert df1.iloc[0][CONSTANTS.CSV_VARS.EPOCH] == pd.Timestamp("2026-01-16T00:00:00")


def test_run_calibration_creates_two_change_points_if_window_contained_with_day(
    tmp_path,
):
    csv_content = (
        "start_date,end_date,quality_flag,quality_bitmask,nan_x,nan_y,nan_z\n"
        "2026-01-16T02:00:00,2026-01-16T04:00:00,1,3,True,False,False\n"
    )
    df1 = run_calibration_on_config_file(
        tmp_path, csv_content, content_date=datetime(2026, 1, 16)
    )

    assert len(df1) == 2
    assert df1.iloc[0][CONSTANTS.CSV_VARS.EPOCH] == pd.Timestamp("2026-01-16T02:00:00")
    assert df1.iloc[0][CONSTANTS.CSV_VARS.QUALITY_FLAG] == 1
    assert np.isnan(df1.iloc[0][CONSTANTS.CSV_VARS.OFFSET_X])
    assert df1.iloc[1][CONSTANTS.CSV_VARS.EPOCH] == pd.Timestamp("2026-01-16T04:00:00")
    assert df1.iloc[1][CONSTANTS.CSV_VARS.QUALITY_FLAG] == -1
    assert df1.iloc[1][CONSTANTS.CSV_VARS.OFFSET_X] == 0.0


def test_run_calibration_raises_without_config(tmp_path):

    params = CalibrationJobParameters(
        date=datetime(2026, 1, 16), mode=ScienceMode.Normal, sensor=Sensor.MAGO
    )
    job = SetQualityAndNaNCalibrationJob(params, tmp_path)
    handler = CalibrationLayerPathHandler(
        descriptor=CalibrationMethod.SET_QUALITY_AND_NAN.short_name,
        content_date=datetime(2026, 1, 16),
    )
    config = CalibrationConfig()  # no set_quality_and_nan

    with pytest.raises(ValueError, match="requires a set_quality_and_nan"):
        job.run_calibration(handler, config)


def test_run_calibration_raises_for_missing_csv(tmp_path):
    params = CalibrationJobParameters(
        date=datetime(2026, 1, 16), mode=ScienceMode.Normal, sensor=Sensor.MAGO
    )
    job = SetQualityAndNaNCalibrationJob(params, tmp_path)
    handler = CalibrationLayerPathHandler(
        descriptor=CalibrationMethod.SET_QUALITY_AND_NAN.short_name,
        content_date=datetime(2026, 1, 16),
    )
    config = CalibrationConfig(
        set_quality_and_nan=SetQualityAndNaNConfig(csv_file="/nonexistent/file.csv")
    )

    with pytest.raises(FileNotFoundError, match="File not found"):
        job.run_calibration(handler, config)


@pytest.mark.parametrize(
    "invalid_flag",
    [2, 3, -2, 100, -100],
)
def test_run_calibration_raises_for_invalid_quality_flag_in_csv(tmp_path, invalid_flag):
    """quality_flag in CSV must be -1, 0, 1, or blank. Other values are rejected."""
    csv_content = (
        "start_date,end_date,quality_flag,quality_bitmask,nan_x,nan_y,nan_z\n"
        f"2026-01-16T02:00:00,2026-01-16T04:00:00,{invalid_flag},3,False,False,False\n"
    )
    config = create_temporary_csv_config(csv_content)
    params = CalibrationJobParameters(
        date=datetime(2026, 1, 16), mode=ScienceMode.Normal, sensor=Sensor.MAGO
    )
    work = tmp_path / "work"
    work.mkdir()
    job = SetQualityAndNaNCalibrationJob(params, work)
    handler = CalibrationLayerPathHandler(
        descriptor=CalibrationMethod.SET_QUALITY_AND_NAN.short_name,
        content_date=datetime(2026, 1, 16),
    )
    with pytest.raises(ValueError, match="quality_flag"):
        job.run_calibration(handler, config)


def test_run_calibration_with_no_matching_windows_creates_empty_layer_with_headers(
    tmp_path,
):
    csv_content = (
        "start_date,end_date,quality_flag,quality_bitmask,nan_x,nan_y,nan_z\n"
        "2026-02-01T00:00:00,2026-02-01T06:00:00,1,3,True,False,False\n"
    )

    df = run_calibration_on_config_file(
        tmp_path, csv_content, content_date=datetime(2026, 1, 16)
    )

    assert len(df) == 0
    assert CONSTANTS.CSV_VARS.EPOCH in df.columns
    assert CONSTANTS.CSV_VARS.QUALITY_FLAG in df.columns
    assert CONSTANTS.CSV_VARS.OFFSET_X in df.columns


def test_get_science_time_range_fallback_without_file(tmp_path):
    """_get_science_time_range should return day boundaries when no science file."""
    params = CalibrationJobParameters(
        date=datetime(2026, 1, 16), mode=ScienceMode.Normal, sensor=Sensor.MAGO
    )
    job = SetQualityAndNaNCalibrationJob(params, tmp_path)

    start, end = job._get_science_time_range(None)
    assert start == datetime(2026, 1, 16, 0, 0, 0)
    assert end == datetime(2026, 1, 17, 0, 0, 0)


def test_get_science_time_range_fallback_nonexistent_file(tmp_path):
    """_get_science_time_range should fall back when the file doesn't exist."""
    params = CalibrationJobParameters(
        date=datetime(2026, 1, 16), mode=ScienceMode.Normal, sensor=Sensor.MAGO
    )
    job = SetQualityAndNaNCalibrationJob(params, tmp_path)

    start, end = job._get_science_time_range(tmp_path / "nonexistent.cdf")
    assert start == datetime(2026, 1, 16, 0, 0, 0)
    assert end == datetime(2026, 1, 17, 0, 0, 0)


def test_calibrate_returns_list_of_paths_to_the_calibration_layer_files(
    quality_csv, tmp_path, temp_datastore, dynamic_work_folder
):
    config = CalibrationConfig(
        set_quality_and_nan=SetQualityAndNaNConfig(csv_file=str(quality_csv))
    )

    results = calibrate(
        start_date=datetime(2026, 1, 16),
        method=CalibrationMethod.SET_QUALITY_AND_NAN,
        mode=ScienceMode.Normal,
        sensor=Sensor.MAGO,
        configuration=config.model_dump_json(),
        save_mode=SaveMode.LocalOnly,
    )

    assert isinstance(results, list)
    assert len(results) == 1
    assert results[0].exists()
    assert "quality" in results[0].name


def test_calibrate_creates_layer_json_and_csv_file(temp_datastore, dynamic_work_folder):
    csv_content = (
        "start_date,end_date,quality_flag,quality_bitmask,nan_x,nan_y,nan_z\n"
        "2026-01-16T02:11:54,2026-01-16T02:11:57,1,3,True,False,False\n"
    )
    date = datetime(2026, 1, 16)
    layer_dir = (
        temp_datastore / "calibration" / "layers" / f"{date.year}" / f"{date.month:02d}"
    )
    config = create_temporary_csv_config(csv_content)
    assert len(list(layer_dir.glob("*quality*"))) == 0

    calibrate(
        start_date=date,
        method=CalibrationMethod.SET_QUALITY_AND_NAN,
        mode=ScienceMode.Normal,
        sensor=Sensor.MAGO,
        configuration=config.model_dump_json(),
        save_mode=SaveMode.LocalOnly,
    )

    layer_files = list(layer_dir.glob("*quality*"))
    assert len(layer_files) == 2, (
        f"Expected 2 files (json + csv) but found {len(layer_files)}: {[f.name for f in layer_files]}"
    )
    expected = [
        "imap_mag_quality-norm-layer-data_20260116_v001.csv",
        "imap_mag_quality-norm-layer_20260116_v001.json",
    ]
    actual = sorted(f.name for f in layer_files)
    assert actual == expected, f"Expected files {expected} but found {actual}"


def test_calibrate_and_apply_set_quality_and_nan_end_to_end(
    temp_datastore,
    dynamic_work_folder,
    spice_kernels,
):
    # Create input CSV with a quality window inside the science data range
    # Science data spans 02:11:51.521 to 02:11:59.021 (16 rows at 0.5s)
    csv_content = (
        "start_date,end_date,quality_flag,quality_bitmask,nan_x,nan_y,nan_z\n"
        "2026-01-16T02:11:54,2026-01-16T02:11:57,1,4,True,False,False\n"
    )
    date = datetime(2026, 1, 16)
    config = create_temporary_csv_config(csv_content)

    files = calibrate(
        start_date=date,
        method=CalibrationMethod.SET_QUALITY_AND_NAN,
        mode=ScienceMode.Normal,
        sensor=Sensor.MAGO,
        configuration=config.model_dump_json(),
        save_mode=SaveMode.LocalOnly,
    )
    apply(
        layers=[f.name for f in files],
        start_date=date,
        mode=ScienceMode.Normal,
        save_mode=SaveMode.LocalOnly,
    )

    output_l2_file = (
        temp_datastore
        / f"science/mag/l2-pre/{date.year}/{date.month:02d}/imap_mag_l2-pre_norm-srf_{date.year}{date.month:02d}{date.day:02d}_v001.cdf"
    )
    assert output_l2_file.exists()

    output_offsets_file = (
        temp_datastore
        / f"science-ancillary/l2-offsets/{date.year}/{date.month:02d}/imap_mag_l2-norm-offsets_{date.year}{date.month:02d}{date.day:02d}_{date.year}{date.month:02d}{date.day:02d}_v001.cdf"
    )
    assert output_offsets_file.exists()

    FILLVAL = CONSTANTS.CDF_FLOAT_FILLVAL

    # Verify the offsets CDF has correct quality flags, bitmask, and FILLVAL offsets
    with open_cdf(output_offsets_file) as cdf:
        quality_flags = cdf["quality_flag"][...]
        quality_bitmask = cdf["quality_bitmask"][...]
        offsets = cdf["offsets"][...]

        # Science timestamps:
        # indices 0-4: before window (02:11:51.521 to 02:11:53.521)
        # indices 5-10: inside window (02:11:54.021 to 02:11:56.521)
        # indices 11-15: after window (02:11:57.021 to 02:11:59.021)

        # Before window: quality_flag=0, bitmask=0, offsets=0
        for i in range(5):
            assert quality_flags[i] == 0, f"Row {i} should have quality_flag=0"
            assert quality_bitmask[i] == 0, f"Row {i} should have quality_bitmask=0"
            assert offsets[i, 0] != FILLVAL, f"Row {i} offset_x should not be FILLVAL"

        # Inside window: quality_flag=1, bitmask=4, offset_x=FILLVAL
        for i in range(5, 11):
            assert quality_flags[i] == 1, f"Row {i} should have quality_flag=1"
            assert quality_bitmask[i] == 4, f"Row {i} should have quality_bitmask=4"
            assert offsets[i, 0] == FILLVAL, f"Row {i} offset_x should be FILLVAL"
            assert offsets[i, 1] != FILLVAL, f"Row {i} offset_y should not be FILLVAL"

        # After window: quality_flag=0, bitmask=0, offsets=0
        for i in range(11, 16):
            assert quality_flags[i] == 0, f"Row {i} should have quality_flag=0"
            assert quality_bitmask[i] == 0, f"Row {i} should have quality_bitmask=0"
            assert offsets[i, 0] != FILLVAL, f"Row {i} offset_x should not be FILLVAL"

    # Verify the L2 output has FILLVAL where expected
    with open_cdf(output_l2_file) as cdf:
        assert "b_srf" in cdf
        vectors = cdf["b_srf"][...]

        # Inside window: x component should be FILLVAL (large absolute value)
        for i in range(5, 11):
            assert abs(float(vectors[i, 0])) > 1e30, (
                f"L2 row {i} x should be FILLVAL, got {vectors[i, 0]}"
            )

        # Outside window: should be normal science values
        for i in range(5):
            assert abs(float(vectors[i, 0])) < 1e30, (
                f"L2 row {i} x should not be FILLVAL"
            )
        for i in range(11, 16):
            assert abs(float(vectors[i, 0])) < 1e30, (
                f"L2 row {i} x should not be FILLVAL"
            )


def test_apply_empty_quality_layer_produces_zero_quality_flags(
    temp_datastore,
    dynamic_work_folder,
    spice_kernels,
):
    # CSV with data for a completely different day - no windows will match Jan 16
    csv_content = (
        "start_date,end_date,quality_flag,quality_bitmask,nan_x,nan_y,nan_z\n"
        "2026-02-01T00:00:00,2026-02-01T06:00:00,1,3,True,False,False\n"
    )
    config = create_temporary_csv_config(csv_content)
    date = datetime(2026, 1, 16)

    calibrate(
        start_date=date,
        method=CalibrationMethod.SET_QUALITY_AND_NAN,
        mode=ScienceMode.Normal,
        sensor=Sensor.MAGO,
        configuration=config.model_dump_json(),
        save_mode=SaveMode.LocalOnly,
    )
    apply(
        layers=["*quality*"],
        start_date=date,
        mode=ScienceMode.Normal,
        save_mode=SaveMode.LocalOnly,
    )

    output_offsets_file = (
        temp_datastore
        / f"science-ancillary/l2-offsets/{date.year}/{date.month:02d}/imap_mag_l2-norm-offsets_{date.year}{date.month:02d}{date.day:02d}_{date.year}{date.month:02d}{date.day:02d}_v001.cdf"
    )
    assert output_offsets_file.exists()

    with open_cdf(output_offsets_file) as cdf:
        quality_flags = cdf["quality_flag"][...]
        quality_bitmask = cdf["quality_bitmask"][...]

        # No quality windows matched - every epoch should be unflagged
        for i, (flag, mask) in enumerate(zip(quality_flags, quality_bitmask)):
            assert flag == 0, f"Row {i} should have quality_flag=0, got {flag}"
            assert mask == 0, f"Row {i} should have quality_bitmask=0, got {mask}"


def create_temporary_csv_config(csv_content):
    csv_path = Path(tempfile.mktemp(suffix=".csv"))
    csv_path.write_text(csv_content)

    config = CalibrationConfig(
        set_quality_and_nan=SetQualityAndNaNConfig(csv_file=str(csv_path))
    )

    return config


def _set_science_cdf_fill_value(path: Path, row_index: int):
    """Overwrite one row's vectors with CDF Fill Value in a science CDF."""
    with open_cdf(path, readonly=False) as cdf:
        vectors = cdf["vectors"][...]
        vectors[row_index, 0] = CONSTANTS.CDF_FLOAT_FILLVAL  # x
        vectors[row_index, 1] = CONSTANTS.CDF_FLOAT_FILLVAL  # y
        vectors[row_index, 2] = CONSTANTS.CDF_FLOAT_FILLVAL  # z
        cdf["vectors"] = vectors


def test_apply_empty_quality_layer_does_not_overwrite_existing_nan(
    temp_datastore,
    dynamic_work_folder,
    spice_kernels,
):
    # CSV with data for a completely different day - no windows will match Jan 16
    csv_content = (
        "start_date,end_date,quality_flag,quality_bitmask,nan_x,nan_y,nan_z\n"
        "2026-02-01T00:00:00,2026-02-01T06:00:00,1,3,True,False,False\n"
    )
    config = create_temporary_csv_config(csv_content)
    date = datetime(2026, 1, 16)
    SCIENCE_FILE = "imap_mag_l1c_norm-mago_20260116_v001.cdf"

    # Row indices we'll inject fill values into
    SCIENCE_FILL_ROW = 3  # row in science CDF set to fill value
    modified_science = temp_datastore / f"science/mag/l1c/2026/01/{SCIENCE_FILE}"
    _set_science_cdf_fill_value(modified_science, SCIENCE_FILL_ROW)

    # Calibrate - should create an empty (headers-only) quality layer
    files = calibrate(
        start_date=date,
        method=CalibrationMethod.SET_QUALITY_AND_NAN,
        mode=ScienceMode.Normal,
        sensor=Sensor.MAGO,
        configuration=config.model_dump_json(),
        save_mode=SaveMode.LocalOnly,
    )

    # Apply the empty quality layer - must not raise
    apply(
        layers=[f.name for f in files],
        start_date=date,
        mode=ScienceMode.Normal,
        save_mode=SaveMode.LocalOnly,
    )

    output_l2_file = (
        temp_datastore
        / f"science/mag/l2-pre/{date.year}/{date.month:02d}/imap_mag_l2-pre_norm-srf_{date.year}{date.month:02d}{date.day:02d}_v001.cdf"
    )
    assert output_l2_file.exists()

    # Verify the L2 output has FILLVAL where expected
    with open_cdf(output_l2_file) as cdf:
        assert "b_srf" in cdf
        vectors = cdf["b_srf"][...]

        # row SCIENCE_FILL_ROW should still be FILLVAL in L2 since the quality layer is empty and should not overwrite existing NaNs
        assert abs(float(vectors[SCIENCE_FILL_ROW, 0])) > 1e30, (
            f"L2 row {SCIENCE_FILL_ROW} x should still be FILLVAL, got {vectors[SCIENCE_FILL_ROW, 0]}"
        )

        # all other rows must not be fill val
        for i in range(len(vectors)):
            if i == SCIENCE_FILL_ROW:
                continue
            assert abs(float(vectors[i, 0])) < 1e30, (
                f"L2 row {i} x should not be FILLVAL, got {vectors[i, 0]}"
            )


def test_apply_empty_quality_layer_on_top_of_existing_flags_and_bitmasks_does_nothing(
    temp_datastore,
    dynamic_work_folder,
    spice_kernels,
):
    # Science data spans 02:11:51.521 to 02:11:59.021 (16 rows at 0.5s)

    # Layer 1 - indices 5-10: inside window (02:11:54.021 to 02:11:56.521) are flagged with quality_flag=1, bitmask=4
    csv_content = (
        "start_date,end_date,quality_flag,quality_bitmask,nan_x,nan_y,nan_z\n"
        "2026-01-16T02:11:54,2026-01-16T02:11:57,1,4,True,False,False\n"
    )
    date = datetime(2026, 1, 16)
    config = create_temporary_csv_config(csv_content)

    files = []
    files += calibrate(
        start_date=date,
        method=CalibrationMethod.SET_QUALITY_AND_NAN,
        mode=ScienceMode.Normal,
        sensor=Sensor.MAGO,
        configuration=config.model_dump_json(),
        save_mode=SaveMode.LocalOnly,
    )

    # Layer 2 - config is outside the window so this should change nothing
    csv_content = (
        "start_date,end_date,quality_flag,quality_bitmask,nan_x,nan_y,nan_z\n"
        "2026-01-17T21:00:54,2026-01-17T21:00:57,1,4,True,False,False\n"
    )
    config = create_temporary_csv_config(csv_content)
    files += calibrate(
        start_date=date,
        method=CalibrationMethod.SET_QUALITY_AND_NAN,
        mode=ScienceMode.Normal,
        sensor=Sensor.MAGO,
        configuration=config.model_dump_json(),
        save_mode=SaveMode.LocalOnly,
    )

    apply(
        layers=[f.name for f in files],
        start_date=date,
        mode=ScienceMode.Normal,
        save_mode=SaveMode.LocalOnly,
    )

    output_l2_file = (
        temp_datastore
        / f"science/mag/l2-pre/{date.year}/{date.month:02d}/imap_mag_l2-pre_norm-srf_{date.year}{date.month:02d}{date.day:02d}_v001.cdf"
    )
    assert output_l2_file.exists()

    output_offsets_file = (
        temp_datastore
        / f"science-ancillary/l2-offsets/{date.year}/{date.month:02d}/imap_mag_l2-norm-offsets_{date.year}{date.month:02d}{date.day:02d}_{date.year}{date.month:02d}{date.day:02d}_v001.cdf"
    )
    assert output_offsets_file.exists()

    FILLVAL = CONSTANTS.CDF_FLOAT_FILLVAL

    # Verify the offsets CDF has correct quality flags, bitmask, and FILLVAL offsets
    with open_cdf(output_offsets_file) as cdf:
        quality_flags = cdf["quality_flag"][...]
        quality_bitmask = cdf["quality_bitmask"][...]
        offsets = cdf["offsets"][...]

        # Science timestamps:
        # indices 0-4: before window (02:11:51.521 to 02:11:53.521)
        # indices 5-10: inside window (02:11:54.021 to 02:11:56.521)
        # indices 11-15: after window (02:11:57.021 to 02:11:59.021)

        # Before window: quality_flag=0, bitmask=0, offsets=0
        for i in range(5):
            assert quality_flags[i] == 0, f"Row {i} should have quality_flag=0"
            assert quality_bitmask[i] == 0, f"Row {i} should have quality_bitmask=0"
            assert offsets[i, 0] != FILLVAL, f"Row {i} offset_x should not be FILLVAL"

        # Inside window: quality_flag=1, bitmask=4, offset_x=FILLVAL
        for i in range(5, 11):
            assert quality_flags[i] == 1, f"Row {i} should have quality_flag=1"
            assert quality_bitmask[i] == 4, f"Row {i} should have quality_bitmask=4"
            assert offsets[i, 0] == FILLVAL, f"Row {i} offset_x should be FILLVAL"
            assert offsets[i, 1] != FILLVAL, f"Row {i} offset_y should not be FILLVAL"

        # After window: quality_flag=0, bitmask=0, offsets=0
        for i in range(11, 16):
            assert quality_flags[i] == 0, f"Row {i} should have quality_flag=0"
            assert quality_bitmask[i] == 0, f"Row {i} should have quality_bitmask=0"
            assert offsets[i, 0] != FILLVAL, f"Row {i} offset_x should not be FILLVAL"


@pytest.mark.parametrize(
    "top_layer_flag,top_layer_bitmask,expected_flag,expected_bitmask",
    [
        # Zero means "no change" via OR semantics — previous layer values propagate through
        ("0", "0", 1, 4),
        ("1", "1", 1, 5),  # different bitmask: 4 | 1 = 5
        (
            "1",
            "7",
            1,
            7,
        ),  # Bitwise OR of bitmask: 4 (from layer 1) | 7 (from layer 2) = 7
        # Blank means "no change" — previous layer values propagate through
        ("", "", 1, 4),
        # -1 flag clears the quality flag to 0
        ("-1", "0", 0, 4),
        # Negative bitmask clears specific bits: -4 clears bit 2 (value 4) → 4 & ~4 = 0
        ("0", "-4", 1, 0),
        # -1 flag AND negative bitmask together clear both
        ("-1", "-4", 0, 0),
        # -65535 clears all 16 bits of the bitmask
        ("0", "-65535", 1, 0),
    ],
)
def test_apply_quality_layer_on_top_of_existing_flags_and_bitmasks_can_override_earlier_layers(
    temp_datastore,
    dynamic_work_folder,
    spice_kernels,
    top_layer_flag: str,
    top_layer_bitmask: str,
    expected_flag: int,
    expected_bitmask: int,
):
    # Science data spans 02:11:51.521 to 02:11:59.021 (16 rows at 0.5s)

    # Layer 1 - indices 5-10: inside window (02:11:54.021 to 02:11:56.521) are flagged with quality_flag=1, bitmask=4
    csv_content = (
        "start_date,end_date,quality_flag,quality_bitmask,nan_x,nan_y,nan_z\n"
        "2026-01-16T02:11:54,2026-01-16T02:11:57,1,4,True,False,False\n"
    )
    date = datetime(2026, 1, 16)
    config = create_temporary_csv_config(csv_content)

    files = []
    files += calibrate(
        start_date=date,
        method=CalibrationMethod.SET_QUALITY_AND_NAN,
        mode=ScienceMode.Normal,
        sensor=Sensor.MAGO,
        configuration=config.model_dump_json(),
        save_mode=SaveMode.LocalOnly,
    )

    # reset them with a second layer that has the same window but back to zero flag/zero mask
    csv_content = (
        "start_date,end_date,quality_flag,quality_bitmask,nan_x,nan_y,nan_z\n"
        f"2026-01-16T02:11:54,2026-01-16T02:11:57,{top_layer_flag},{top_layer_bitmask},True,False,False\n"
    )
    config = create_temporary_csv_config(csv_content)
    files += calibrate(
        start_date=date,
        method=CalibrationMethod.SET_QUALITY_AND_NAN,
        mode=ScienceMode.Normal,
        sensor=Sensor.MAGO,
        configuration=config.model_dump_json(),
        save_mode=SaveMode.LocalOnly,
    )

    apply(
        layers=[f.name for f in files],
        start_date=date,
        mode=ScienceMode.Normal,
        save_mode=SaveMode.LocalOnly,
    )

    output_l2_file = (
        temp_datastore
        / f"science/mag/l2-pre/{date.year}/{date.month:02d}/imap_mag_l2-pre_norm-srf_{date.year}{date.month:02d}{date.day:02d}_v001.cdf"
    )
    assert output_l2_file.exists()

    output_offsets_file = (
        temp_datastore
        / f"science-ancillary/l2-offsets/{date.year}/{date.month:02d}/imap_mag_l2-norm-offsets_{date.year}{date.month:02d}{date.day:02d}_{date.year}{date.month:02d}{date.day:02d}_v001.cdf"
    )
    assert output_offsets_file.exists()

    FILLVAL = CONSTANTS.CDF_FLOAT_FILLVAL

    # Verify the offsets CDF has correct quality flags, bitmask, and FILLVAL offsets
    with open_cdf(output_offsets_file) as cdf:
        quality_flags = cdf["quality_flag"][...]
        quality_bitmask = cdf["quality_bitmask"][...]
        offsets = cdf["offsets"][...]

        # Science timestamps:
        # indices 0-4: before window (02:11:51.521 to 02:11:53.521)
        # indices 5-10: inside window (02:11:54.021 to 02:11:56.521)
        # indices 11-15: after window (02:11:57.021 to 02:11:59.021)

        # Before window: quality_flag=0, bitmask=0, offsets=0
        for i in range(5):
            assert quality_flags[i] == 0, f"Row {i} should have quality_flag=0"
            assert quality_bitmask[i] == 0, f"Row {i} should have quality_bitmask=0"
            assert offsets[i, 0] != FILLVAL, f"Row {i} offset_x should not be FILLVAL"

        # Inside window: quality_flag=1, bitmask=4, offset_x=FILLVAL
        for i in range(5, 11):
            assert quality_flags[i] == expected_flag, (
                f"Row {i} should have quality_flag={expected_flag}"
            )
            assert quality_bitmask[i] == expected_bitmask, (
                f"Row {i} should have quality_bitmask={expected_bitmask}"
            )
            assert offsets[i, 0] == FILLVAL, f"Row {i} offset_x should be FILLVAL"
            assert offsets[i, 1] != FILLVAL, f"Row {i} offset_y should not be FILLVAL"

        # After window: quality_flag=0, bitmask=0, offsets=0
        for i in range(11, 16):
            assert quality_flags[i] == 0, f"Row {i} should have quality_flag=0"
            assert quality_bitmask[i] == 0, f"Row {i} should have quality_bitmask=0"
            assert offsets[i, 0] != FILLVAL, f"Row {i} offset_x should not be FILLVAL"


def test_quality_calibration_csv_resolved_from_cwd(monkeypatch, tmp_path):
    csv = tmp_path / "my_quality_events.csv"
    csv.write_text(
        "start_date,end_date,quality_flag,quality_bitmask,nan_x,nan_y,nan_z\n"
        "2026-01-16T02:00:00,2026-01-16T04:00:00,1,3,True,False,False\n"
    )

    # Change CWD to the directory that contains the CSV so the bare filename resolves
    monkeypatch.chdir(tmp_path)

    # Use a separate directory as the datastore so the CSV is NOT there
    datastore = tmp_path / "store"
    datastore.mkdir()
    params = CalibrationJobParameters(
        date=datetime(2026, 1, 16), mode=ScienceMode.Normal, sensor=Sensor.MAGO
    )
    work = tmp_path / "work"
    work.mkdir()
    job = SetQualityAndNaNCalibrationJob(params, work)
    job.setup_datastore(datastore)  # CSV is not here
    handler = CalibrationLayerPathHandler(
        descriptor=CalibrationMethod.SET_QUALITY_AND_NAN.short_name,
        content_date=datetime(2026, 1, 16),
    )
    config = CalibrationConfig(
        set_quality_and_nan=SetQualityAndNaNConfig(csv_file="my_quality_events.csv")
    )

    # Should succeed: resolver finds the file via CWD fallback
    calfile, datafile = job.run_calibration(handler, config)

    assert calfile.exists()
    assert datafile.exists()
    df = pd.read_csv(datafile)
    assert len(df) == 2  # start + end change points
