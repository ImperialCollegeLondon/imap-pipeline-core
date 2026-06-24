from datetime import datetime

import pytest

from imap_mag.io.FileFinder import FileFinder
from imap_mag.util import MAGSensor, ScienceMode


@pytest.fixture
def datastore(tmp_path):
    """Create a minimal datastore with layer and science files for testing."""
    # Create layer files with multiple versions
    layer_dir = tmp_path / "calibration" / "layers" / "2026" / "01"
    layer_dir.mkdir(parents=True)

    # Noop layers: v001 and v002
    (layer_dir / "imap_mag_noop-norm-layer_20260116_v001.json").touch()
    (layer_dir / "imap_mag_noop-norm-layer-data_20260116_v001.csv").touch()
    (layer_dir / "imap_mag_noop-norm-layer_20260116_v002.json").touch()
    (layer_dir / "imap_mag_noop-norm-layer-data_20260116_v002.csv").touch()
    (layer_dir / "imap_mag_noop-burst-layer_20260116_v001.json").touch()
    (layer_dir / "imap_mag_noop-burst-layer-data_20260116_v001.csv").touch()
    (layer_dir / "imap_mag_noop-burst-layer_20260116_v002.json").touch()
    (layer_dir / "imap_mag_noop-burst-layer-data_20260116_v002.csv").touch()

    # Set-quality layer: v000
    (layer_dir / "imap_mag_quality-norm-layer_20260116_v000.json").touch()
    (layer_dir / "imap_mag_quality-norm-layer-data_20260116_v000.csv").touch()
    (layer_dir / "imap_mag_quality-burst-layer_20260116_v000.json").touch()
    (layer_dir / "imap_mag_quality-burst-layer-data_20260116_v000.csv").touch()

    # Science files
    l1c_dir = tmp_path / "science" / "mag" / "l1c" / "2026" / "01"
    l1c_dir.mkdir(parents=True)
    (l1c_dir / "imap_mag_l1c_norm-mago_20260116_v001.cdf").touch()
    (l1c_dir / "imap_mag_l1c_norm-mago_20260117_v001.cdf").touch()
    (l1c_dir / "imap_mag_l1c_norm-magi_20260116_v001.cdf").touch()

    l1b_dir = tmp_path / "science" / "mag" / "l1b" / "2026" / "01"
    l1b_dir.mkdir(parents=True)
    (l1b_dir / "imap_mag_l1b_norm-mago_20260116_v000.cdf").touch()
    (l1b_dir / "imap_mag_l1b_burst-mago_20260116_v002.cdf").touch()

    return tmp_path


class TestResolveLayerPatterns:
    def test_exact_filename_passes_through(self, datastore):
        finder = FileFinder(datastore)
        result = finder.find_layers_by_date_and_patterns(
            ["imap_mag_noop-burst-layer_20260116_v001.json"],
            datetime(2026, 1, 16),
            ScienceMode.Burst,
        )
        assert result == ["imap_mag_noop-burst-layer_20260116_v001.json"]

    def test_wildcard_returns_highest_version_per_descriptor(self, datastore):
        finder = FileFinder(datastore)
        result = finder.find_layers_by_date_and_patterns(
            ["*noop*"],
            datetime(2026, 1, 16),
            ScienceMode.Normal,
        )
        # Should only return v002, not v001
        assert result == ["imap_mag_noop-norm-layer_20260116_v002.json"]

    def test_wildcard_star_returns_all_descriptors_highest_versions(self, datastore):
        finder = FileFinder(datastore)
        result = finder.find_layers_by_date_and_patterns(
            ["*"],
            datetime(2026, 1, 16),
            ScienceMode.Normal,
        )
        # Should return highest version of each descriptor
        assert "imap_mag_noop-norm-layer_20260116_v002.json" in result
        assert "imap_mag_quality-norm-layer_20260116_v000.json" in result
        assert len(result) == 2

    def test_no_match_returns_empty(self, datastore):
        finder = FileFinder(datastore)
        result = finder.find_layers_by_date_and_patterns(
            ["*nonexistent*"],
            datetime(2026, 1, 16),
            ScienceMode.Normal,
        )
        assert result == []

    def test_missing_directory_returns_empty(self, datastore):
        finder = FileFinder(datastore)
        result = finder.find_layers_by_date_and_patterns(
            ["*"],
            datetime(2025, 3, 15),
            ScienceMode.Normal,
        )
        assert result == []

    def test_mixed_exact_and_wildcard(self, datastore):
        finder = FileFinder(datastore)
        result = finder.find_layers_by_date_and_patterns(
            ["imap_mag_noop-burst-layer_20260116_v001.json", "*quality*"],
            datetime(2026, 1, 16),
            ScienceMode.Burst,
        )
        # Exact filename passes through even if not highest version
        assert result == [
            "imap_mag_noop-burst-layer_20260116_v001.json",
            "imap_mag_quality-burst-layer_20260116_v000.json",
        ]


class TestKeepHighestVersions:
    def test_keeps_highest_version(self):
        filenames = [
            "imap_mag_noop-layer_20260116_v001.json",
            "imap_mag_noop-layer_20260116_v002.json",
            "imap_mag_noop-layer_20260116_v000.json",
        ]
        result = FileFinder._keep_highest_version_layers_only(filenames)
        assert result == ["imap_mag_noop-layer_20260116_v002.json"]

    def test_multiple_descriptors(self):
        filenames = [
            "imap_mag_noop-layer_20260116_v001.json",
            "imap_mag_noop-layer_20260116_v003.json",
            "imap_mag_quality-layer_20260116_v000.json",
            "imap_mag_quality-layer_20260116_v001.json",
        ]
        result = FileFinder._keep_highest_version_layers_only(filenames)
        assert len(result) == 2
        assert "imap_mag_noop-layer_20260116_v003.json" in result
        assert "imap_mag_quality-layer_20260116_v001.json" in result

    def test_empty_list(self):
        assert FileFinder._keep_highest_version_layers_only([]) == []

    def test_unparseable_filenames_skipped(self):
        filenames = [
            "imap_mag_noop-layer_20260116_v001.json",
            "not_a_valid_layer_file.json",
        ]
        result = FileFinder._keep_highest_version_layers_only(filenames)
        assert result == ["imap_mag_noop-layer_20260116_v001.json"]


class TestFindScienceFile:
    def test_normal_mode_prefers_l1c(self, datastore):
        finder = FileFinder(datastore)

        result = finder.find_latest_science_by_date(
            datetime(2026, 1, 16), ScienceMode.Normal, MAGSensor.OBS
        )
        assert result == "imap_mag_l1c_norm-mago_20260116_v001.cdf"

        result = finder.find_latest_science_by_date(
            datetime(2026, 1, 16), ScienceMode.Normal, MAGSensor.IBS
        )
        assert result == "imap_mag_l1c_norm-magi_20260116_v001.cdf"

    def test_normal_mode_checks_correct_sensor(self, datastore):
        finder = FileFinder(datastore)
        with pytest.raises(FileNotFoundError):
            finder.find_latest_science_by_date(
                datetime(2026, 1, 17), ScienceMode.Normal, MAGSensor.IBS
            )

    def test_normal_mode_falls_back_to_l1b(self, tmp_path):
        """When no L1C exists, normal mode should fall back to L1B."""
        l1b_dir = tmp_path / "science" / "mag" / "l1b" / "2026" / "01"
        l1b_dir.mkdir(parents=True)
        (l1b_dir / "imap_mag_l1b_norm-mago_20260116_v000.cdf").touch()

        finder = FileFinder(tmp_path)
        result = finder.find_latest_science_by_date(
            datetime(2026, 1, 16), ScienceMode.Normal, MAGSensor.OBS
        )
        assert result == "imap_mag_l1b_norm-mago_20260116_v000.cdf"

    def test_burst_mode_uses_l1b_only(self, datastore):
        finder = FileFinder(datastore)
        result = finder.find_latest_science_by_date(
            datetime(2026, 1, 16), ScienceMode.Burst, MAGSensor.OBS
        )
        assert result == "imap_mag_l1b_burst-mago_20260116_v002.cdf"

    def test_burst_mode_ignores_l1c(self, tmp_path):
        """Burst mode should not use L1C even if available."""
        l1c_dir = tmp_path / "science" / "mag" / "l1c" / "2026" / "01"
        l1c_dir.mkdir(parents=True)
        (l1c_dir / "imap_mag_l1c_burst-mago_20260116_v001.cdf").touch()

        finder = FileFinder(tmp_path)
        with pytest.raises(FileNotFoundError):
            finder.find_latest_science_by_date(
                datetime(2026, 1, 16), ScienceMode.Burst, MAGSensor.OBS
            )

    def test_highest_version_returned(self, tmp_path):
        l1c_dir = tmp_path / "science" / "mag" / "l1c" / "2026" / "01"
        l1c_dir.mkdir(parents=True)
        (l1c_dir / "imap_mag_l1c_norm-mago_20260116_v000.cdf").touch()
        (l1c_dir / "imap_mag_l1c_norm-mago_20260116_v003.cdf").touch()
        (l1c_dir / "imap_mag_l1c_norm-mago_20260116_v001.cdf").touch()

        finder = FileFinder(tmp_path)
        result = finder.find_latest_science_by_date(
            datetime(2026, 1, 16), ScienceMode.Normal, MAGSensor.OBS
        )
        assert result == "imap_mag_l1c_norm-mago_20260116_v003.cdf"

    def test_non_cdf_files_ignored(self, tmp_path):
        """Non-CDF files in the science directory should be ignored."""
        l1c_dir = tmp_path / "science" / "mag" / "l1c" / "2026" / "01"
        l1c_dir.mkdir(parents=True)
        (l1c_dir / "imap_mag_l1c_norm-mago_20260116_v001.cdf").touch()
        (l1c_dir / "imap_mag_l1c_norm-mago_20260116_v001.txt").touch()
        (l1c_dir / "README.md").touch()

        finder = FileFinder(tmp_path)
        result = finder.find_latest_science_by_date(
            datetime(2026, 1, 16), ScienceMode.Normal, MAGSensor.OBS
        )
        assert result == "imap_mag_l1c_norm-mago_20260116_v001.cdf"

    def test_no_science_dir_raises(self, tmp_path):
        finder = FileFinder(tmp_path)
        with pytest.raises(FileNotFoundError, match="Science directory"):
            finder.find_latest_science_by_date(
                datetime(2026, 1, 16), ScienceMode.Normal, MAGSensor.OBS
            )

    def test_no_matching_file_raises(self, tmp_path):
        science_dir = tmp_path / "science" / "mag" / "l1c" / "2026" / "01"
        science_dir.mkdir(parents=True)
        # File for wrong date
        (science_dir / "imap_mag_l1c_norm-mago_20260115_v001.cdf").touch()

        finder = FileFinder(tmp_path)
        with pytest.raises(FileNotFoundError, match="No science file found"):
            finder.find_latest_science_by_date(
                datetime(2026, 1, 16), ScienceMode.Normal, MAGSensor.OBS
            )
