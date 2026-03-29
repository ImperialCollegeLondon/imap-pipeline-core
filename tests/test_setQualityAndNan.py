import tempfile
import threading
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from imap_mag.cli.apply import apply
from imap_mag.cli.calibrate import calibrate
from imap_mag.config import SaveMode
from imap_mag.config.CalibrationConfig import CalibrationConfig, SetQualityAndNaNConfig
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import (
    CalibrationLayer,
    CalibrationMethod,
    Sensor,
    SetQualityAndNaNCalibrationJob,
)
from mag_toolkit.calibration.CalibrationDefinitions import CONSTANTS, ValueType
from mag_toolkit.calibration.CalibrationJobParameters import CalibrationJobParameters

with threading.Lock():
    from spacepy import pycdf


@pytest.fixture
def quality_csv(tmp_path):
    """Create a CSV file with quality/NaN specifications."""
    csv_content = (
        "start_date,end_date,quality_flag,quality_bitmask,nan_x,nan_y,nan_z\n"
        "2026-01-16T02:11:54,2026-01-16T02:11:57,2,3,True,False,False\n"
    )
    csv_path = tmp_path / "quality_input.csv"
    csv_path.write_text(csv_content)
    return csv_path


def test_calibration_job_creates_interpolation_layer(quality_csv, tmp_path):
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

    from imap_mag.io.file import CalibrationLayerPathHandler

    cal_handler = CalibrationLayerPathHandler(
        descriptor=CalibrationMethod.SET_QUALITY_AND_NAN.short_name,
        content_date=datetime(2026, 1, 16),
    )

    job = SetQualityAndNaNCalibrationJob(params, work_folder)
    calfile, datafile = job.run_calibration(cal_handler, config)

    assert calfile.exists()
    assert datafile.exists()

    layer = CalibrationLayer.from_file(calfile, load_contents=True)
    assert layer.value_type == ValueType.INTERPOLATION_POINTS
    assert layer.method == CalibrationMethod.SET_QUALITY_AND_NAN

    df = pd.read_csv(datafile, parse_dates=[CONSTANTS.CSV_VARS.EPOCH])
    assert len(df) == 2

    start_row = df.iloc[0]
    assert start_row[CONSTANTS.CSV_VARS.QUALITY_FLAG] == 2
    assert start_row[CONSTANTS.CSV_VARS.QUALITY_BITMASK] == 3
    assert np.isnan(start_row[CONSTANTS.CSV_VARS.OFFSET_X])
    assert start_row[CONSTANTS.CSV_VARS.OFFSET_Y] == 0.0
    assert start_row[CONSTANTS.CSV_VARS.OFFSET_Z] == 0.0

    end_row = df.iloc[1]
    assert end_row[CONSTANTS.CSV_VARS.QUALITY_FLAG] == 0
    assert end_row[CONSTANTS.CSV_VARS.QUALITY_BITMASK] == 0
    assert end_row[CONSTANTS.CSV_VARS.OFFSET_X] == 0.0
    assert end_row[CONSTANTS.CSV_VARS.OFFSET_Y] == 0.0
    assert end_row[CONSTANTS.CSV_VARS.OFFSET_Z] == 0.0


def test_calibration_job_splits_across_days(tmp_path):
    """Verify a window spanning two days creates change points for each day."""
    csv_content = (
        "start_date,end_date,quality_flag,quality_bitmask,nan_x,nan_y,nan_z\n"
        "2026-01-16T20:00:00,2026-01-17T06:00:00,4,5,False,True,True\n"
    )
    csv_path = tmp_path / "multi_day.csv"
    csv_path.write_text(csv_content)

    config = CalibrationConfig(
        set_quality_and_nan=SetQualityAndNaNConfig(csv_file=str(csv_path))
    )
    work_folder = tmp_path / "work"
    work_folder.mkdir()

    from imap_mag.io.file import CalibrationLayerPathHandler

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
    assert df1.iloc[0][CONSTANTS.CSV_VARS.QUALITY_FLAG] == 4
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
    assert df2.iloc[0][CONSTANTS.CSV_VARS.QUALITY_FLAG] == 4
    assert df2.iloc[1][CONSTANTS.CSV_VARS.QUALITY_FLAG] == 0


def test_calibrate_and_apply_set_quality_and_nan_end_to_end(
    temp_datastore,
    dynamic_work_folder,
    spice_kernels,
):
    """End-to-end: calibrate creates INTERPOLATION layer, apply uses it.

    Verifies quality flags and NaN are applied only within the specified
    time window, and data outside the window is unaltered.
    """
    # Create input CSV with a quality window inside the science data range
    # Science data spans 02:11:51.521 to 02:11:59.021 (16 rows at 0.5s)
    csv_content = (
        "start_date,end_date,quality_flag,quality_bitmask,nan_x,nan_y,nan_z\n"
        "2026-01-16T02:11:54,2026-01-16T02:11:57,2,3,True,False,False\n"
    )
    csv_path = Path(tempfile.mktemp(suffix=".csv"))
    csv_path.write_text(csv_content)

    try:
        config = CalibrationConfig(
            set_quality_and_nan=SetQualityAndNaNConfig(csv_file=str(csv_path))
        )

        # Step 1: Calibrate - create the SET_QUALITY_AND_NAN layer
        calibrate(
            date=datetime(2026, 1, 16),
            method=CalibrationMethod.SET_QUALITY_AND_NAN,
            mode=ScienceMode.Normal,
            sensor=Sensor.MAGO,
            configuration=config.model_dump_json(),
            save_mode=SaveMode.LocalOnly,
        )

        # Verify the layer was created in the datastore
        layer_dir = temp_datastore / "calibration" / "layers" / "2026" / "01"
        layer_files = list(layer_dir.glob("*set-quality-and-nan*"))
        assert len(layer_files) >= 2  # json + csv

        # Step 2: Apply the SET_QUALITY_AND_NAN layer
        apply(
            layers=["*set-quality-and-nan*"],
            date=datetime(2026, 1, 16),
            mode=ScienceMode.Normal,
            save_mode=SaveMode.LocalOnly,
        )

        date = datetime(2026, 1, 16)
        output_l2_file = (
            temp_datastore
            / f"science/mag/l2-pre/{date.year}/{date.month:02d}/imap_mag_l2-pre_norm-srf_{date.year}{date.month:02d}{date.day:02d}_v000.cdf"
        )
        assert output_l2_file.exists()

        output_offsets_file = (
            temp_datastore
            / f"science-ancillary/l2-offsets/{date.year}/{date.month:02d}/imap_mag_l2-norm-offsets_{date.year}{date.month:02d}{date.day:02d}_{date.year}{date.month:02d}{date.day:02d}_v000.cdf"
        )
        assert output_offsets_file.exists()

        FILLVAL = CONSTANTS.CDF_FLOAT_FILLVAL

        # Verify the offsets CDF has correct quality flags, bitmask, and FILLVAL offsets
        with pycdf.CDF(str(output_offsets_file)) as cdf:
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
                assert offsets[i, 0] != FILLVAL, (
                    f"Row {i} offset_x should not be FILLVAL"
                )

            # Inside window: quality_flag=2, bitmask=3, offset_x=FILLVAL
            for i in range(5, 11):
                assert quality_flags[i] == 2, f"Row {i} should have quality_flag=2"
                assert quality_bitmask[i] == 3, f"Row {i} should have quality_bitmask=3"
                assert offsets[i, 0] == FILLVAL, f"Row {i} offset_x should be FILLVAL"
                assert offsets[i, 1] != FILLVAL, (
                    f"Row {i} offset_y should not be FILLVAL"
                )

            # After window: quality_flag=0, bitmask=0, offsets=0
            for i in range(11, 16):
                assert quality_flags[i] == 0, f"Row {i} should have quality_flag=0"
                assert quality_bitmask[i] == 0, f"Row {i} should have quality_bitmask=0"
                assert offsets[i, 0] != FILLVAL, (
                    f"Row {i} offset_x should not be FILLVAL"
                )

        # Verify the L2 output has FILLVAL where expected
        with pycdf.CDF(str(output_l2_file)) as cdf:
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

    finally:
        csv_path.unlink(missing_ok=True)
