"""Tests for SciencePathHandler."""

from datetime import datetime
from pathlib import Path

import numpy as np
import xarray as xr
from cdflib.xarray import xarray_to_cdf

from imap_mag.io.file.SciencePathHandler import SciencePathHandler
from tests.util.miscellaneous import open_cdf


def _write_science_cdf(folder: Path, filename: str) -> Path:
    path = folder / filename
    dataset = xr.Dataset(
        {"b_srf": ("epoch", np.array([1.5, 2.5, 3.5]))},
        attrs={
            "Logical_source": "imap_mag_l2_norm-srf",
            "Data_version": "001.0001",
            "Logical_file_id": path.stem,
            "Parents": ["offsets.cdf", "l1c.cdf"],
        },
    )
    xarray_to_cdf(dataset, str(path))
    return path


def _read_global_attrs(cdf_file: Path) -> dict[str, list[str]]:
    with open_cdf(cdf_file) as cdf:
        return {name: [str(v) for v in cdf.attrs[name]] for name in cdf.attrs}


def _l2_pre_handler(version: int, version_major: int = 1) -> SciencePathHandler:
    return SciencePathHandler(
        level="l2-pre",
        descriptor="norm-srf",
        content_date=datetime(2026, 1, 16),
        version=version,
        version_major=version_major,
    )


class TestPrepareForVersion:
    def test_stamps_data_version_and_logical_file_id_in_place(self, tmp_path):
        source = _write_science_cdf(
            tmp_path, "imap_mag_l2-pre_norm-srf_20260116_v001.0001.cdf"
        )

        stamped = _l2_pre_handler(version=2, version_major=3).prepare_for_version(
            source
        )

        assert stamped == source
        attrs = _read_global_attrs(source)
        assert attrs["Data_version"] == ["003.0002"]
        assert attrs["Logical_file_id"] == [
            "imap_mag_l2-pre_norm-srf_20260116_v003.0002"
        ]

    def test_keeps_other_attributes_and_data_unchanged(self, tmp_path):
        source = _write_science_cdf(
            tmp_path, "imap_mag_l2-pre_norm-srf_20260116_v001.0001.cdf"
        )

        _l2_pre_handler(version=2).prepare_for_version(source)

        attrs = _read_global_attrs(source)
        assert attrs["Logical_source"] == ["imap_mag_l2_norm-srf"]
        assert attrs["Parents"] == ["offsets.cdf", "l1c.cdf"]
        with open_cdf(source) as cdf:
            assert cdf["b_srf"][...].tolist() == [1.5, 2.5, 3.5]

    def test_leaves_file_unchanged_when_version_unchanged(self, tmp_path):
        source = _write_science_cdf(
            tmp_path, "imap_mag_l2-pre_norm-srf_20260116_v001.0001.cdf"
        )
        before = source.read_bytes()

        assert _l2_pre_handler(version=1).prepare_for_version(source) == source
        assert source.read_bytes() == before

    def test_never_rewrites_other_science_levels(self, tmp_path):
        source = _write_science_cdf(
            tmp_path, "imap_mag_l1c_norm-mago_20260116_v001.0001.cdf"
        )
        handler = SciencePathHandler(
            level="l1c",
            descriptor="norm-mago",
            content_date=datetime(2026, 1, 16),
            version=2,
            version_major=1,
        )

        before = source.read_bytes()

        assert handler.prepare_for_version(source) == source
        assert source.read_bytes() == before
