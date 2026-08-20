"""Unit tests for the calibrate-convert CLI command (src/imap_mag/cli/convert.py)."""

from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from imap_mag.cli.apply import FileType
from imap_mag.cli.convert import convert
from imap_mag.config import AppSettings, ConvertCommandConfig, SaveMode
from imap_mag.io.file import CalibrationLayerPathHandler
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import ConversionStrategy, LayerDataFormat
from mag_toolkit.calibration.CalibrationDefinitions import (
    CONSTANTS,
    CalibrationMetadata,
    CalibrationMethod,
    Mission,
    Sensor,
    Validity,
    ValueType,
)
from mag_toolkit.calibration.CalibrationLayer import CalibrationLayer

pytestmark = pytest.mark.usefixtures("clean_datastore", "dynamic_work_folder")


def _precise_df(seed: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            CONSTANTS.CSV_VARS.EPOCH: pd.to_datetime(
                [f"2026-01-16T01:02:03.{123456789 + seed:09d}"[:29]]
            ),
            CONSTANTS.CSV_VARS.OFFSET_X: [1.0000001 + seed],
            CONSTANTS.CSV_VARS.OFFSET_Y: [-2.0000002 + seed],
            CONSTANTS.CSV_VARS.OFFSET_Z: [3.14159265 + seed],
            CONSTANTS.CSV_VARS.TIMEDELTA: [0.0],
            CONSTANTS.CSV_VARS.QUALITY_FLAG: [0],
            CONSTANTS.CSV_VARS.QUALITY_BITMASK: [0],
        }
    )


def _make_layer(
    datastore_root,
    descriptor: str,
    date: datetime,
    version: int,
    fmt: FileType,
    seed: int = 0,
    version_major: int = 1,
):
    """Write a real calibration layer (JSON [+companion]) directly in the datastore."""
    folder = (
        datastore_root
        / "calibration"
        / "layers"
        / date.strftime("%Y")
        / date.strftime("%m")
    )
    folder.mkdir(parents=True, exist_ok=True)

    df = _precise_df(seed)
    layer = CalibrationLayer(
        id="",
        mission=Mission.IMAP,
        validity=Validity(
            start=df[CONSTANTS.CSV_VARS.EPOCH].iloc[0],
            end=df[CONSTANTS.CSV_VARS.EPOCH].iloc[0],
        ),
        sensor=Sensor.MAGO,
        version=version,
        version_major=version_major,
        metadata=CalibrationMetadata(
            dependencies=[],
            science=[],
            creation_timestamp=np.datetime64("now"),
            content_date=np.datetime64(date),
        ),
        value_type=ValueType.VECTOR,
        method=CalibrationMethod.NOOP,
    )
    layer._contents = df

    handler = CalibrationLayerPathHandler(
        descriptor=descriptor,
        content_date=date,
        version=version,
        version_major=version_major,
        extension="cdf" if fmt == FileType.CDF else "json",
    )
    primary_path = folder / handler.get_filename()

    if fmt != FileType.CDF:
        data_handler = handler.create_new_datafile_handler(LayerDataFormat(fmt.value))
        layer.metadata.data_filename = __import__("pathlib").Path(
            data_handler.get_filename()
        )

    layer.write_to_file(primary_path)
    return primary_path, df


def _read_df(path) -> pd.DataFrame:
    layer = CalibrationLayer.from_file(path, load_contents=True)
    assert layer._contents is not None
    return layer._contents


class TestConvertFormatPairs:
    @pytest.mark.parametrize(
        "source_fmt,target_fmt",
        [
            (FileType.CSV, FileType.PARQUET),
            (FileType.PARQUET, FileType.CSV),
            (FileType.CSV, FileType.CDF),
            (FileType.CDF, FileType.CSV),
            (FileType.PARQUET, FileType.CDF),
            (FileType.CDF, FileType.PARQUET),
        ],
    )
    def test_converts_each_format_pair(self, source_fmt, target_fmt):
        date = datetime(2026, 1, 16)
        data_store = AppSettings().data_store

        primary_path, original_df = _make_layer(
            data_store, "quality-norm", date, 1, source_fmt, seed=1
        )

        converted = convert(
            input_layers=[primary_path.name],
            output_layer_data_format=target_fmt,
            output_layer_versioning_strategy=ConversionStrategy.OVERWRITE,
            save_mode=SaveMode.LocalOnly,
        )

        assert converted, "convert() should return at least one published file"
        new_primary = next(
            p
            for p in converted
            if p.suffix == (".cdf" if target_fmt == FileType.CDF else ".json")
        )
        assert new_primary.exists()

        converted_df = _read_df(new_primary)

        involves_cdf = source_fmt == FileType.CDF or target_fmt == FileType.CDF
        epoch_col = CONSTANTS.CSV_VARS.EPOCH
        assert (
            pd.to_datetime(converted_df[epoch_col]).tolist()
            == pd.to_datetime(original_df[epoch_col]).tolist()
        )

        for col in [
            CONSTANTS.CSV_VARS.OFFSET_X,
            CONSTANTS.CSV_VARS.OFFSET_Y,
            CONSTANTS.CSV_VARS.OFFSET_Z,
        ]:
            if involves_cdf:
                assert converted_df[col].iloc[0] == pytest.approx(
                    original_df[col].iloc[0], rel=1e-6
                )
            else:
                assert converted_df[col].iloc[0] == original_df[col].iloc[0]


class TestConvertSkipsSameFormat:
    def test_skip_when_already_target_format(self):
        date = datetime(2026, 1, 16)
        data_store = AppSettings().data_store
        primary_path, _ = _make_layer(
            data_store, "quality-norm", date, 1, FileType.PARQUET
        )

        result = convert(
            input_layers=[primary_path.name],
            output_layer_data_format=FileType.PARQUET,
            save_mode=SaveMode.LocalOnly,
        )

        assert result == []
        # Original files untouched.
        assert primary_path.exists()


class TestConvertOverwriteStrategy:
    def test_overwrite_removes_old_format_companion(self):
        date = datetime(2026, 1, 16)
        data_store = AppSettings().data_store
        primary_path, _ = _make_layer(data_store, "quality-norm", date, 1, FileType.CSV)
        old_companion = primary_path.parent / (
            "imap_mag_quality-norm-layer-data_20260116_v001.0001.csv"
        )
        assert old_companion.exists()

        converted = convert(
            input_layers=[primary_path.name],
            output_layer_data_format=FileType.PARQUET,
            output_layer_versioning_strategy=ConversionStrategy.OVERWRITE,
            save_mode=SaveMode.LocalOnly,
        )

        assert not old_companion.exists(), "old .csv companion must be removed"
        new_json = primary_path.parent / (
            "imap_mag_quality-norm-layer_20260116_v001.0001.json"
        )
        new_companion = primary_path.parent / (
            "imap_mag_quality-norm-layer-data_20260116_v001.0001.parquet"
        )
        assert new_json.exists()
        assert new_companion.exists()
        assert set(converted) == {new_json, new_companion}


class TestConvertCreateNewVersionStrategy:
    def test_create_new_version_keeps_originals(self):
        date = datetime(2026, 1, 16)
        data_store = AppSettings().data_store
        primary_path, _ = _make_layer(data_store, "quality-norm", date, 1, FileType.CSV)
        old_companion = primary_path.parent / (
            "imap_mag_quality-norm-layer-data_20260116_v001.0001.csv"
        )

        convert(
            input_layers=[primary_path.name],
            output_layer_data_format=FileType.PARQUET,
            output_layer_versioning_strategy=ConversionStrategy.CREATE_NEW_VERSION,
            save_mode=SaveMode.LocalOnly,
        )

        assert primary_path.exists(), "original JSON must be left in place"
        assert old_companion.exists(), "original CSV must be left in place"
        new_json = primary_path.parent / (
            "imap_mag_quality-norm-layer_20260116_v001.0002.json"
        )
        assert new_json.exists()


class TestConvertPatternAndDateSearch:
    def test_finds_layer_with_no_date_given(self):
        date = datetime(2026, 1, 16)
        data_store = AppSettings().data_store
        _make_layer(data_store, "quality-norm", date, 1, FileType.CSV)

        converted = convert(
            input_layers=["*quality-norm*"],
            output_layer_data_format=FileType.PARQUET,
            save_mode=SaveMode.LocalOnly,
        )
        assert converted

    def test_date_range_excludes_layers_outside_window(self):
        data_store = AppSettings().data_store
        _make_layer(data_store, "quality-norm", datetime(2026, 1, 16), 1, FileType.CSV)

        # No layer falls inside the window, and convert() raises (like apply())
        # rather than silently converting nothing.
        with pytest.raises(FileNotFoundError):
            convert(
                input_layers=["*quality-norm*"],
                start_date=datetime(2026, 2, 1),
                end_date=datetime(2026, 2, 28),
                output_layer_data_format=FileType.PARQUET,
                save_mode=SaveMode.LocalOnly,
            )

    def test_mode_filter_restricts_to_matching_mode(self):
        data_store = AppSettings().data_store
        _make_layer(data_store, "quality-burst", datetime(2026, 1, 16), 1, FileType.CSV)

        # Only a burst-mode layer exists, so filtering for norm finds nothing
        # and convert() raises (like apply()) rather than silently converting nothing.
        with pytest.raises(FileNotFoundError):
            convert(
                input_layers=["*"],
                mode=ScienceMode.Normal,
                output_layer_data_format=FileType.PARQUET,
                save_mode=SaveMode.LocalOnly,
            )


class TestConvertPublishToDataStoreFalse:
    def test_publish_false_leaves_only_work_folder_copy(self, monkeypatch):
        date = datetime(2026, 1, 16)
        data_store = AppSettings().data_store
        primary_path, _ = _make_layer(data_store, "quality-norm", date, 1, FileType.CSV)

        settings = AppSettings(
            convert=ConvertCommandConfig(publish_to_data_store=False)
        )
        monkeypatch.setattr("imap_mag.cli.convert.AppSettings", lambda: settings)

        converted = convert(
            input_layers=[primary_path.name],
            output_layer_data_format=FileType.PARQUET,
            save_mode=SaveMode.LocalOnly,
        )

        assert converted
        for path in converted:
            assert settings.work_folder in path.parents

        # Datastore must be untouched: original files still there, no new parquet published.
        assert primary_path.exists()
        published_companion = primary_path.parent / (
            "imap_mag_quality-norm-layer-data_20260116_v001.0001.parquet"
        )
        assert not published_companion.exists()
