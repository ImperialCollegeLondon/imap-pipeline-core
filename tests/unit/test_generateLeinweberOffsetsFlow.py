"""Unit tests for generate_leinweber_offsets_flow."""

from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from imap_mag.cli.generateLeinweberOffsets import ConfigureMidpoint
from imap_mag.io import FileFinder
from imap_mag.util import DatetimeProvider
from mag_toolkit.calibration import Sensor
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.generateLeinweberOffsetsFlow import (
    _existing_offsets,
    _science_ready,
    _window_days,
    generate_leinweber_offsets_flow,
    generate_leinweber_offsets_flow_run_name,
)

MODULE = "prefect_server.generateLeinweberOffsetsFlow"
DAY = datetime(2026, 1, 30)
DEFAULT_MIDPOINT = ConfigureMidpoint.USE_12_HRS_AFTER_PIVOT_PLATFORM_MOVEMENT


class TestGenerateLeinweberOffsetsFlowRunName:
    def test_with_date_range(self):
        mock_params = {
            "start_date": date(2025, 1, 1),
            "end_date": date(2025, 1, 31),
            "duration_hours": 24,
        }
        with patch(f"{MODULE}.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            result = generate_leinweber_offsets_flow_run_name()

        assert "01-01-2025" in result
        assert "31-01-2025" in result
        assert "24hr" in result

    def test_without_end_date(self):
        mock_params = {
            "start_date": date(2025, 1, 15),
            "end_date": None,
            "duration_hours": 24,
        }
        with patch(f"{MODULE}.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            result = generate_leinweber_offsets_flow_run_name()

        assert result == "Leinweber-offsets-15-01-2025-24hr"

    def test_auto_backfill_label_when_start_date_omitted(self):
        with patch(f"{MODULE}.flow_run") as mock_flow_run:
            mock_flow_run.parameters = {"duration_hours": 24}
            result = generate_leinweber_offsets_flow_run_name()

        assert result == "Leinweber-offsets-auto-backfill-24hr"


def _patch_settings(tmp_path, backfill_lookback_days: int = 14):
    mock_settings = MagicMock()
    mock_settings.work_folder = tmp_path
    mock_settings.data_store = tmp_path / "datastore"
    mock_settings.calibrate.leinweber.backfill_lookback_days = backfill_lookback_days
    return mock_settings


class TestGenerateLeinweberOffsetsFlow:
    def test_raises_when_end_date_before_start_date(self, tmp_path):
        with patch(f"{MODULE}.AppSettings", return_value=_patch_settings(tmp_path)):
            with pytest.raises(ValueError, match="cannot be before start date"):
                generate_leinweber_offsets_flow.fn(
                    start_date=datetime(2025, 1, 5),
                    end_date=datetime(2025, 1, 1),
                )

    def test_defaults_to_rolling_backfill_window_when_no_dates_given(self, tmp_path):
        fixed_provider = DatetimeProvider(fixed_now=datetime(2026, 3, 10))
        checked_days: list[datetime] = []

        def record_existing(datastore_finder, day, duration_hours, sensors):
            checked_days.append(day)
            return None

        with (
            patch(
                f"{MODULE}.AppSettings",
                return_value=_patch_settings(tmp_path, backfill_lookback_days=5),
            ),
            patch(f"{MODULE}._existing_offsets", side_effect=record_existing),
            patch(f"{MODULE}._science_ready", return_value=False),
            patch(f"{MODULE}.DatetimeProvider", return_value=fixed_provider),
        ):
            result = generate_leinweber_offsets_flow.fn()

        # "Yesterday" relative to the fixed now (2026-03-10) is 2026-03-09; a
        # 5-day lookback spans 2026-03-05..2026-03-09 inclusive.
        assert checked_days == [
            datetime(2026, 3, 5),
            datetime(2026, 3, 6),
            datetime(2026, 3, 7),
            datetime(2026, 3, 8),
            datetime(2026, 3, 9),
        ]
        assert result == []

    def test_backfill_lookback_days_parameter_overrides_the_config_default(
        self, tmp_path
    ):
        fixed_provider = DatetimeProvider(fixed_now=datetime(2026, 3, 10))
        checked_days: list[datetime] = []

        def record_existing(datastore_finder, day, duration_hours, sensors):
            checked_days.append(day)
            return None

        with (
            # Config default is 5, but the explicit parameter below (2) should win.
            patch(
                f"{MODULE}.AppSettings",
                return_value=_patch_settings(tmp_path, backfill_lookback_days=5),
            ),
            patch(f"{MODULE}._existing_offsets", side_effect=record_existing),
            patch(f"{MODULE}._science_ready", return_value=False),
            patch(f"{MODULE}.DatetimeProvider", return_value=fixed_provider),
        ):
            generate_leinweber_offsets_flow.fn(backfill_lookback_days=2)

        assert checked_days == [datetime(2026, 3, 8), datetime(2026, 3, 9)]

    def test_requires_matlab_repo_when_a_day_needs_generating(self, tmp_path):
        with (
            patch(f"{MODULE}.AppSettings", return_value=_patch_settings(tmp_path)),
            patch(f"{MODULE}._existing_offsets", return_value=None),
            patch(f"{MODULE}._science_ready", return_value=True),
        ):
            with pytest.raises(TypeError, match="Unsupported matlab_repo type"):
                generate_leinweber_offsets_flow.fn(
                    start_date=datetime(2026, 1, 30),
                    matlab_repo=None,
                )

    def test_calls_generate_leinweber_offsets_once_per_day(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()

        with (
            patch(f"{MODULE}.AppSettings", return_value=_patch_settings(tmp_path)),
            patch(f"{MODULE}._existing_offsets", return_value=None),
            patch(f"{MODULE}._science_ready", return_value=True),
            patch(
                f"{MODULE}.generate_leinweber_offsets",
                return_value=[Path("offsets.csv")],
            ) as mock_generate,
        ):
            result = generate_leinweber_offsets_flow.fn(
                start_date=datetime(2026, 1, 30),
                end_date=datetime(2026, 2, 1),
                matlab_repo=str(repo),
            )

        assert mock_generate.call_count == 3
        assert result == [Path("offsets.csv")] * 3
        first_call_date = mock_generate.call_args_list[0].kwargs["date"]
        assert first_call_date == datetime(2026, 1, 30)
        assert mock_generate.call_args_list[0].kwargs["configure_midpoint"] == (
            DEFAULT_MIDPOINT
        )

    def test_passes_through_a_non_default_configure_midpoint(self, tmp_path):
        # configure_midpoint is MATLAB's concern (see generate_leinweber_offsets);
        # the flow's own readiness check (_science_ready) doesn't need or use it -
        # this only checks it reaches the generate_leinweber_offsets CLI call.
        repo = tmp_path / "repo"
        repo.mkdir()

        with (
            patch(f"{MODULE}.AppSettings", return_value=_patch_settings(tmp_path)),
            patch(f"{MODULE}._existing_offsets", return_value=None),
            patch(f"{MODULE}._science_ready", return_value=True),
            patch(
                f"{MODULE}.generate_leinweber_offsets",
                return_value=[Path("offsets.csv")],
            ) as mock_generate,
        ):
            generate_leinweber_offsets_flow.fn(
                start_date=datetime(2026, 1, 30),
                matlab_repo=str(repo),
                configure_midpoint=ConfigureMidpoint.USE_DATE_AS_IS,
            )

        assert mock_generate.call_args.kwargs["configure_midpoint"] == (
            ConfigureMidpoint.USE_DATE_AS_IS
        )

    def test_skips_days_with_offsets_already_published(self, tmp_path):
        existing_path = Path("already-there.csv")

        with (
            patch(f"{MODULE}.AppSettings", return_value=_patch_settings(tmp_path)),
            patch(f"{MODULE}._existing_offsets", return_value=[existing_path]),
            patch(f"{MODULE}._science_ready") as mock_ready,
            patch(f"{MODULE}.generate_leinweber_offsets") as mock_generate,
        ):
            result = generate_leinweber_offsets_flow.fn(
                start_date=datetime(2026, 1, 30),
            )

        # Already-published short-circuits before the readiness check or any
        # MATLAB invocation.
        mock_ready.assert_not_called()
        mock_generate.assert_not_called()
        assert result == [existing_path]

    def test_force_regenerate_bypasses_the_already_published_skip(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        existing_path = Path("already-there.csv")

        with (
            patch(f"{MODULE}.AppSettings", return_value=_patch_settings(tmp_path)),
            patch(
                f"{MODULE}._existing_offsets", return_value=[existing_path]
            ) as mock_existing,
            patch(f"{MODULE}._science_ready", return_value=True),
            patch(
                f"{MODULE}.generate_leinweber_offsets",
                return_value=[Path("regenerated.csv")],
            ) as mock_generate,
        ):
            result = generate_leinweber_offsets_flow.fn(
                start_date=datetime(2026, 1, 30),
                matlab_repo=str(repo),
                force_regenerate=True,
            )

        # _existing_offsets is never even consulted when forcing.
        mock_existing.assert_not_called()
        mock_generate.assert_called_once()
        assert result == [Path("regenerated.csv")]

    def test_passes_through_allow_pivot_platform_extrapolation(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()

        with (
            patch(f"{MODULE}.AppSettings", return_value=_patch_settings(tmp_path)),
            patch(f"{MODULE}._existing_offsets", return_value=None),
            patch(f"{MODULE}._science_ready", return_value=True),
            patch(
                f"{MODULE}.generate_leinweber_offsets",
                return_value=[Path("offsets.csv")],
            ) as mock_generate,
        ):
            generate_leinweber_offsets_flow.fn(
                start_date=datetime(2026, 1, 30),
                matlab_repo=str(repo),
                allow_pivot_platform_extrapolation=True,
            )

        assert (
            mock_generate.call_args.kwargs["allow_pivot_platform_extrapolation"] is True
        )

    def test_skips_days_where_science_is_not_ready(self, tmp_path):
        with (
            patch(f"{MODULE}.AppSettings", return_value=_patch_settings(tmp_path)),
            patch(f"{MODULE}._existing_offsets", return_value=None),
            patch(f"{MODULE}._science_ready", return_value=False),
            patch(f"{MODULE}.generate_leinweber_offsets") as mock_generate,
            patch(f"{MODULE}._resolve_matlab_repo_path") as mock_resolve,
        ):
            result = generate_leinweber_offsets_flow.fn(
                start_date=datetime(2026, 1, 30),
            )

        mock_generate.assert_not_called()
        # matlab_repo is never even resolved when there is nothing pending.
        mock_resolve.assert_not_called()
        assert result == []

    def test_mixed_batch_only_generates_pending_days(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        existing_path = Path("day1-already-there.csv")

        def fake_existing(datastore_finder, day, duration_hours, sensors):
            return [existing_path] if day == datetime(2026, 1, 30) else None

        def fake_ready(datastore_finder, day, duration_hours, sensors):
            return day == datetime(2026, 1, 31)

        with (
            patch(f"{MODULE}.AppSettings", return_value=_patch_settings(tmp_path)),
            patch(f"{MODULE}._existing_offsets", side_effect=fake_existing),
            patch(f"{MODULE}._science_ready", side_effect=fake_ready),
            patch(
                f"{MODULE}.generate_leinweber_offsets",
                return_value=[Path("generated.csv")],
            ) as mock_generate,
        ):
            result = generate_leinweber_offsets_flow.fn(
                start_date=datetime(2026, 1, 30),
                end_date=datetime(2026, 2, 1),
                matlab_repo=str(repo),
            )

        # Day 1 already published, day 2 ready and generated, day 3 not ready.
        mock_generate.assert_called_once()
        assert mock_generate.call_args.kwargs["date"] == datetime(2026, 1, 31)
        assert result == [existing_path, Path("generated.csv")]


class TestGenerateLeinweberOffsetsFlowSplitByDay:
    def _make_flow_run(self, name):
        fake = MagicMock()
        fake.name = name
        return fake

    def test_splits_range_into_per_day_deployment_runs(self):
        with (
            patch(
                "prefect_server.calibrationFlowCommon.run_deployment",
                side_effect=lambda **kwargs: self._make_flow_run(
                    str(kwargs["parameters"]["start_date"])
                ),
            ) as mock_run_deployment,
            patch(f"{MODULE}.generate_leinweber_offsets") as mock_generate,
        ):
            result = generate_leinweber_offsets_flow.fn(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 3),
                matlab_repo="some-repo",
                split_by_day=True,
            )

        assert mock_generate.call_count == 0
        assert mock_run_deployment.call_count == 3
        for call in mock_run_deployment.call_args_list:
            assert call.kwargs["name"] == (
                f"{PREFECT_CONSTANTS.FLOW_NAMES.GENERATE_LEINWEBER_OFFSETS}/"
                f"{PREFECT_CONSTANTS.DEPLOYMENT_NAMES.GENERATE_LEINWEBER_OFFSETS}"
            )
            assert call.kwargs["parameters"]["split_by_day"] is False
            assert call.kwargs["parameters"]["configure_midpoint"] == DEFAULT_MIDPOINT
            assert call.kwargs["parameters"]["backfill_lookback_days"] is None
            assert call.kwargs["parameters"]["force_regenerate"] is False
            assert (
                call.kwargs["parameters"]["allow_pivot_platform_extrapolation"] is False
            )
        assert len(result) == 3

    def test_single_day_does_not_split(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()

        with (
            patch(
                "prefect_server.calibrationFlowCommon.run_deployment"
            ) as mock_run_deployment,
            patch(f"{MODULE}.AppSettings", return_value=_patch_settings(tmp_path)),
            patch(f"{MODULE}._existing_offsets", return_value=None),
            patch(f"{MODULE}._science_ready", return_value=True),
            patch(
                f"{MODULE}.generate_leinweber_offsets",
                return_value=[Path("offsets.csv")],
            ) as mock_generate,
        ):
            generate_leinweber_offsets_flow.fn(
                start_date=datetime(2026, 1, 30),
                matlab_repo=str(repo),
                split_by_day=True,
            )

        mock_run_deployment.assert_not_called()
        mock_generate.assert_called_once()


class TestWindowDays:
    # Strategy-agnostic and deliberately conservative: the midpoint could fall
    # anywhere within [day's midnight, day's midnight + 24h) depending on
    # configure_midpoint, so the window always spans at least the day before and
    # the day after `day`, regardless of duration.

    def test_24_hour_duration_touches_the_day_before_and_after(self):
        assert _window_days(DAY, 24) == [
            datetime(2026, 1, 29),
            datetime(2026, 1, 30),
            datetime(2026, 1, 31),
        ]

    def test_short_duration_still_touches_the_day_before_and_after(self):
        # A 1-hour window doesn't shrink the touched days below the 3-day
        # baseline: the midpoint alone could already be anywhere within `day`.
        assert _window_days(DAY, 1) == [
            datetime(2026, 1, 29),
            datetime(2026, 1, 30),
            datetime(2026, 1, 31),
        ]

    def test_long_duration_widens_the_touched_days_further(self):
        assert _window_days(DAY, 72) == [
            datetime(2026, 1, 28),
            datetime(2026, 1, 29),
            datetime(2026, 1, 30),
            datetime(2026, 1, 31),
            datetime(2026, 2, 1),
        ]


class TestScienceReady:
    def _science_file(self, datastore: Path, sensor: str, day: datetime) -> None:
        folder = datastore / "science" / "mag" / "l1c" / f"{day:%Y}" / f"{day:%m}"
        folder.mkdir(parents=True, exist_ok=True)
        (folder / f"imap_mag_l1c_norm-{sensor}_{day:%Y%m%d}_v001.cdf").write_text("x")

    _TOUCHED_DAYS = (
        datetime(2026, 1, 29),
        datetime(2026, 1, 30),
        datetime(2026, 1, 31),
    )

    def test_false_when_datastore_empty(self, tmp_path):
        finder = FileFinder(tmp_path)
        assert _science_ready(finder, DAY, 24, [Sensor.MAGO, Sensor.MAGI]) is False

    def test_false_when_only_target_day_present_not_neighbours(self, tmp_path):
        self._science_file(tmp_path, "mago", DAY)
        self._science_file(tmp_path, "magi", DAY)
        finder = FileFinder(tmp_path)
        assert _science_ready(finder, DAY, 24, [Sensor.MAGO, Sensor.MAGI]) is False

    def test_false_when_only_one_sensor_present_for_all_touched_days(self, tmp_path):
        for day in self._TOUCHED_DAYS:
            self._science_file(tmp_path, "mago", day)
        finder = FileFinder(tmp_path)
        assert _science_ready(finder, DAY, 24, [Sensor.MAGO, Sensor.MAGI]) is False

    def test_true_when_all_touched_days_present_for_all_sensors(self, tmp_path):
        for day in self._TOUCHED_DAYS:
            self._science_file(tmp_path, "mago", day)
            self._science_file(tmp_path, "magi", day)
        finder = FileFinder(tmp_path)
        assert _science_ready(finder, DAY, 24, [Sensor.MAGO, Sensor.MAGI]) is True

    def test_true_when_only_requesting_the_sensor_present(self, tmp_path):
        for day in self._TOUCHED_DAYS:
            self._science_file(tmp_path, "mago", day)
        finder = FileFinder(tmp_path)
        assert _science_ready(finder, DAY, 24, [Sensor.MAGO]) is True


class TestExistingOffsets:
    def _offsets_file(self, datastore: Path, sensor: str) -> Path:
        folder = datastore / "calibration" / "calculated_offsets" / "leinweber"
        folder.mkdir(parents=True, exist_ok=True)
        path = (
            folder / f"imap_mag_{sensor}-raw-leinweber-offsets-24hr_20260130_v001.csv"
        )
        path.write_text("x")
        return path

    def test_none_when_datastore_empty(self, tmp_path):
        finder = FileFinder(tmp_path)
        assert _existing_offsets(finder, DAY, 24, [Sensor.MAGO, Sensor.MAGI]) is None

    def test_none_when_only_one_sensor_published(self, tmp_path):
        self._offsets_file(tmp_path, "mago")
        finder = FileFinder(tmp_path)
        assert _existing_offsets(finder, DAY, 24, [Sensor.MAGO, Sensor.MAGI]) is None

    def test_returns_paths_when_all_sensors_published(self, tmp_path):
        mago_path = self._offsets_file(tmp_path, "mago")
        magi_path = self._offsets_file(tmp_path, "magi")
        finder = FileFinder(tmp_path)

        result = _existing_offsets(finder, DAY, 24, [Sensor.MAGO, Sensor.MAGI])

        assert result is not None
        assert set(result) == {mago_path, magi_path}

    def test_does_not_match_a_different_duration(self, tmp_path):
        self._offsets_file(tmp_path, "mago")  # written as 24hr
        self._offsets_file(tmp_path, "magi")
        finder = FileFinder(tmp_path)

        assert _existing_offsets(finder, DAY, 12, [Sensor.MAGO, Sensor.MAGI]) is None
