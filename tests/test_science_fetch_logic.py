"""Tests for FetchScience download logic and science CLI parameter validation."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from imap_mag.cli.fetch.science import _validate_and_complete_parameters
from imap_mag.download.FetchScience import FetchScience
from imap_mag.util import MAGSensor, ReferenceFrame, ScienceLevel, ScienceMode


class TestValidateAndCompleteParameters:
    def test_all_none_returns_none_tuple(self):
        modes, sensors, frames = _validate_and_complete_parameters(
            ScienceLevel.l2, None, None, None
        )
        assert modes is None
        assert sensors is None
        assert frames is None

    def test_empty_lists_normalized_to_none(self):
        modes, sensors, frames = _validate_and_complete_parameters(
            ScienceLevel.l2, [], [], []
        )
        assert modes is None
        assert sensors is None
        assert frames is None

    def test_none_list_normalized_to_none(self):
        modes, sensors, frames = _validate_and_complete_parameters(
            ScienceLevel.l2, [None], [None], [None]
        )
        assert modes is None
        assert sensors is None
        assert frames is None

    def test_l1b_with_frames_raises(self):
        with pytest.raises(ValueError, match="does not use reference frames"):
            _validate_and_complete_parameters(
                ScienceLevel.l1b,
                [ScienceMode.Normal],
                None,
                [ReferenceFrame.GSE],
            )

    def test_l1b_modes_only_adds_default_sensors(self):
        modes, sensors, frames = _validate_and_complete_parameters(
            ScienceLevel.l1b, [ScienceMode.Normal], None, None
        )
        assert sensors is not None
        assert MAGSensor.IBS in sensors
        assert MAGSensor.OBS in sensors

    def test_l1b_sensors_only_adds_default_modes(self):
        modes, sensors, frames = _validate_and_complete_parameters(
            ScienceLevel.l1b, None, [MAGSensor.IBS], None
        )
        assert modes is not None
        assert ScienceMode.Normal in modes
        assert ScienceMode.Burst in modes

    def test_l2_with_sensors_raises(self):
        with pytest.raises(ValueError, match="does not use sensors"):
            _validate_and_complete_parameters(
                ScienceLevel.l2, None, [MAGSensor.IBS], None
            )

    def test_l2_modes_only_adds_default_reference_frames(self):
        modes, sensors, frames = _validate_and_complete_parameters(
            ScienceLevel.l2, [ScienceMode.Normal], None, None
        )
        assert frames is not None
        assert ReferenceFrame.GSE in frames
        assert ReferenceFrame.DSRF in frames

    def test_l2_frames_only_adds_default_modes(self):
        modes, sensors, frames = _validate_and_complete_parameters(
            ScienceLevel.l2, None, None, [ReferenceFrame.GSE]
        )
        assert modes is not None
        assert ScienceMode.Normal in modes
        assert ScienceMode.Burst in modes

    def test_l1d_modes_only_adds_reference_frames(self):
        modes, sensors, frames = _validate_and_complete_parameters(
            ScienceLevel.l1d, [ScienceMode.Normal], None, None
        )
        assert frames is not None

    def test_l1c_sensors_only_adds_default_modes(self):
        modes, sensors, frames = _validate_and_complete_parameters(
            ScienceLevel.l1c, None, [MAGSensor.OBS], None
        )
        assert modes is not None
        assert ScienceMode.Normal in modes

    def test_l1a_modes_and_sensors_returned_unchanged(self):
        input_modes = [ScienceMode.Normal]
        input_sensors = [MAGSensor.IBS]
        modes, sensors, frames = _validate_and_complete_parameters(
            ScienceLevel.l1a, input_modes, input_sensors, None
        )
        assert modes == input_modes
        assert sensors == input_sensors


class TestFetchScienceGetDescriptors:
    def test_l2_with_modes_and_frames_returns_combinations(self):
        fetcher = FetchScience(MagicMock())
        descriptors = fetcher.get_descriptors(
            level=ScienceLevel.l2,
            modes=[ScienceMode.Normal, ScienceMode.Burst],
            sensors=None,
            reference_frames=[ReferenceFrame.GSE],
        )
        assert len(descriptors) == 2
        assert any("gse" in d for d in descriptors)

    def test_l2_with_none_modes_and_frames_returns_none_list(self):
        fetcher = FetchScience(MagicMock())
        descriptors = fetcher.get_descriptors(
            level=ScienceLevel.l2,
            modes=None,
            sensors=None,
            reference_frames=None,
        )
        assert descriptors == [None]

    def test_l2_only_modes_raises(self):
        fetcher = FetchScience(MagicMock())
        with pytest.raises(ValueError, match="Both modes and reference_frames"):
            fetcher.get_descriptors(
                level=ScienceLevel.l2,
                modes=[ScienceMode.Normal],
                sensors=None,
                reference_frames=None,
            )

    def test_l1b_with_modes_and_sensors_returns_combinations(self):
        fetcher = FetchScience(MagicMock())
        descriptors = fetcher.get_descriptors(
            level=ScienceLevel.l1b,
            modes=[ScienceMode.Normal],
            sensors=[MAGSensor.IBS, MAGSensor.OBS],
            reference_frames=None,
        )
        assert len(descriptors) == 2

    def test_l1b_only_modes_raises(self):
        fetcher = FetchScience(MagicMock())
        with pytest.raises(ValueError, match="Both modes and sensors"):
            fetcher.get_descriptors(
                level=ScienceLevel.l1b,
                modes=[ScienceMode.Normal],
                sensors=None,
                reference_frames=None,
            )

    def test_none_level_returns_none_list(self):
        fetcher = FetchScience(MagicMock())
        descriptors = fetcher.get_descriptors(
            level=None, modes=None, sensors=None, reference_frames=None
        )
        assert descriptors == [None]


class TestFetchScienceDownload:
    def _make_file_detail(self, file_path="mag/l2/test.cdf", descriptor="norm-gse"):
        return {
            "file_path": file_path,
            "descriptor": descriptor,
            "start_date": "20250101",
            "ingestion_date": "20250102 12:00:00",
            "version": "v001",
        }

    def test_raises_when_max_downloads_is_zero(self, tmp_path):
        mock_access = MagicMock()
        fetcher = FetchScience(mock_access)
        with pytest.raises(ValueError, match="max_downloads must be greater than zero"):
            fetcher.download_science(
                level=ScienceLevel.l2,
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 31),
                max_downloads=0,
            )

    def test_raises_when_skip_count_negative(self, tmp_path):
        mock_access = MagicMock()
        fetcher = FetchScience(mock_access)
        with pytest.raises(ValueError, match="skip_items_count must be zero or greater"):
            fetcher.download_science(
                level=ScienceLevel.l2,
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 31),
                skip_items_count=-1,
            )

    def test_returns_empty_when_no_files_found(self, tmp_path):
        mock_access = MagicMock()
        mock_access.query_sdc_files.return_value = []
        fetcher = FetchScience(mock_access)

        result = fetcher.download_science(
            level=ScienceLevel.l2,
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 1, 31),
        )
        assert result == {}

    def test_downloads_file_and_creates_path_handler(self, tmp_path):
        mock_access = MagicMock()
        file_detail = self._make_file_detail()
        mock_access.query_sdc_files.return_value = [file_detail]

        downloaded_path = tmp_path / "test.cdf"
        downloaded_path.write_bytes(b"cdf content")
        mock_access.download.return_value = downloaded_path

        fetcher = FetchScience(mock_access)
        result = fetcher.download_science(
            level=ScienceLevel.l2,
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 1, 31),
        )

        assert len(result) == 1
        assert downloaded_path in result

    def test_skips_empty_downloaded_files(self, tmp_path):
        mock_access = MagicMock()
        file_detail = self._make_file_detail()
        mock_access.query_sdc_files.return_value = [file_detail]

        empty_path = tmp_path / "empty.cdf"
        empty_path.write_bytes(b"")
        mock_access.download.return_value = empty_path

        fetcher = FetchScience(mock_access)
        result = fetcher.download_science(
            level=ScienceLevel.l2,
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 1, 31),
        )

        assert result == {}

    def test_respects_max_downloads_limit(self, tmp_path):
        mock_access = MagicMock()
        file_details = [
            self._make_file_detail(f"mag/l2/test{i}.cdf", "norm-gse") for i in range(5)
        ]
        mock_access.query_sdc_files.return_value = file_details

        def make_file(path):
            p = tmp_path / Path(path).name
            p.write_bytes(b"cdf content")
            return p

        mock_access.download.side_effect = lambda path: make_file(path)

        fetcher = FetchScience(mock_access)
        result = fetcher.download_science(
            level=ScienceLevel.l2,
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 1, 31),
            max_downloads=2,
        )

        assert len(result) == 2

    def test_skips_files_based_on_skip_count(self, tmp_path):
        mock_access = MagicMock()
        file_details = [
            self._make_file_detail(f"mag/l2/test{i}.cdf", "norm-gse") for i in range(3)
        ]
        mock_access.query_sdc_files.return_value = file_details

        call_count = [0]

        def make_file(path):
            call_count[0] += 1
            p = tmp_path / Path(path).name
            p.write_bytes(b"cdf content")
            return p

        mock_access.download.side_effect = lambda path: make_file(path)

        fetcher = FetchScience(mock_access)
        result = fetcher.download_science(
            level=ScienceLevel.l2,
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 1, 31),
            skip_items_count=2,
        )

        assert len(result) == 1
        assert call_count[0] == 1

    def test_uses_ingestion_date_filter_when_requested(self, tmp_path):
        mock_access = MagicMock()
        mock_access.query_sdc_files.return_value = []
        fetcher = FetchScience(mock_access)

        fetcher.download_science(
            level=ScienceLevel.l2,
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 1, 31),
            use_ingestion_date=True,
        )

        call_kwargs = mock_access.query_sdc_files.call_args.kwargs
        assert "ingestion_start_date" in call_kwargs
        assert "ingestion_end_date" in call_kwargs
        assert "start_date" not in call_kwargs
