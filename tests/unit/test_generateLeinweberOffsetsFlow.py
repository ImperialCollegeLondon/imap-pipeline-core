"""Unit tests for generate_leinweber_offsets_flow."""

from datetime import date, datetime
from pathlib import Path
from typing import ClassVar
from unittest.mock import MagicMock, patch

import pytest

from imap_mag.cli.generateLeinweberOffsets import (
    ConfigureMidpoint,
    NoOffsetsProducedError,
)
from imap_mag.io import FileFinder
from imap_mag.util import DatetimeProvider
from mag_toolkit.calibration import Sensor
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.generateLeinweberOffsetsFlow import (
    BackfillLeinweberRunParameters,
    LeinweberByDateRunParameters,
    _create_offsets_table_artifact,
    _existing_offsets,
    _file_has_gap_in_window,
    _pivot_platform_complete,
    _science_ready,
    _window_bounds,
    _window_days,
    generate_leinweber_offsets_flow,
    generate_leinweber_offsets_flow_run_name,
)

MODULE = "prefect_server.generateLeinweberOffsetsFlow"
DAY = datetime(2026, 1, 30)
DEFAULT_MIDPOINT = ConfigureMidpoint.USE_12_HRS_AFTER_PIVOT_PLATFORM_MOVEMENT
LEINWEBER_VARIABLES = PREFECT_CONSTANTS.GENERATE_LEINWEBER_OFFSETS


@pytest.fixture(autouse=True)
def prefect_variables():
    """Stand-in for the Prefect variable store: tests set values by name here,
    and unset names fall back to the default passed to ``Variable.get``."""
    values: dict[str, object] = {}

    def fake_get(name, default=None):
        return values.get(name, default)

    with patch(f"{MODULE}.Variable.get", side_effect=fake_get):
        yield values


class TestGenerateLeinweberOffsetsFlowRunName:
    def test_with_date_range(self):
        mock_params = {
            "run_parameters": LeinweberByDateRunParameters(
                start_date=date(2025, 1, 1), end_date=date(2025, 1, 31)
            ),
            "duration_str": "24h",
        }
        with patch(f"{MODULE}.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            result = generate_leinweber_offsets_flow_run_name()

        assert "01-01-2025" in result
        assert "31-01-2025" in result
        assert "24h" in result

    def test_without_end_date(self):
        mock_params = {
            "run_parameters": LeinweberByDateRunParameters(
                start_date=date(2025, 1, 15)
            ),
            "duration_str": "24h",
        }
        with patch(f"{MODULE}.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            result = generate_leinweber_offsets_flow_run_name()

        assert result == "Leinweber-offsets-15-01-2025-24h"

    def test_auto_backfill_label_when_start_date_omitted(self):
        with patch(f"{MODULE}.flow_run") as mock_flow_run:
            mock_flow_run.parameters = {
                "run_parameters": BackfillLeinweberRunParameters(),
                "duration_str": "24h",
            }
            result = generate_leinweber_offsets_flow_run_name()

        assert result == "Leinweber-offsets-auto-backfill-24h-window-duration"

    def test_falls_back_to_duration_variable_when_unset(self, prefect_variables):
        prefect_variables[LEINWEBER_VARIABLES.DURATION_HOURS_VARIABLE] = 36
        with patch(f"{MODULE}.flow_run") as mock_flow_run:
            mock_flow_run.parameters = {
                "run_parameters": LeinweberByDateRunParameters(
                    start_date=date(2025, 1, 15)
                ),
            }
            result = generate_leinweber_offsets_flow_run_name()

        assert result == "Leinweber-offsets-15-01-2025-36h"


def _patch_settings(tmp_path):
    mock_settings = MagicMock()
    mock_settings.work_folder = tmp_path
    mock_settings.data_store = tmp_path / "datastore"
    return mock_settings


class TestGenerateLeinweberOffsetsFlow:
    @pytest.fixture(autouse=True)
    def _mock_offsets_artifact(self):
        # These tests pass around placeholder paths like Path("offsets.csv") that
        # are never written to disk, so the real artifact function (which reads
        # each path as a CSV) is mocked out here; TestCreateOffsetsTableArtifact
        # below tests it directly against real CSV fixtures.
        with patch(f"{MODULE}._create_offsets_table_artifact") as mock:
            yield mock

    def test_raises_when_end_date_before_start_date(self, tmp_path):
        with patch(f"{MODULE}.AppSettings", return_value=_patch_settings(tmp_path)):
            with pytest.raises(ValueError, match="cannot be before start date"):
                generate_leinweber_offsets_flow.fn(
                    run_parameters=LeinweberByDateRunParameters(
                        start_date=datetime(2025, 1, 5),
                        end_date=datetime(2025, 1, 1),
                    ),
                )

    def test_defaults_to_rolling_backfill_window_when_no_dates_given(
        self, tmp_path, prefect_variables
    ):
        prefect_variables[LEINWEBER_VARIABLES.BACKFILL_LOOKBACK_DAYS_VARIABLE] = 5
        fixed_provider = DatetimeProvider(fixed_now=datetime(2026, 3, 10))
        checked_days: list[datetime] = []

        def record_existing(datastore_finder, day, duration_hours, sensors):
            checked_days.append(day)
            return None

        with (
            patch(
                f"{MODULE}.AppSettings",
                return_value=_patch_settings(tmp_path),
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

    def test_backfill_lookback_days_parameter_overrides_the_variable(
        self, tmp_path, prefect_variables
    ):
        # Variable is 5, but the explicit parameter below (2) should win.
        prefect_variables[LEINWEBER_VARIABLES.BACKFILL_LOOKBACK_DAYS_VARIABLE] = 5
        fixed_provider = DatetimeProvider(fixed_now=datetime(2026, 3, 10))
        checked_days: list[datetime] = []

        def record_existing(datastore_finder, day, duration_hours, sensors):
            checked_days.append(day)
            return None

        with (
            patch(
                f"{MODULE}.AppSettings",
                return_value=_patch_settings(tmp_path),
            ),
            patch(f"{MODULE}._existing_offsets", side_effect=record_existing),
            patch(f"{MODULE}._science_ready", return_value=False),
            patch(f"{MODULE}.DatetimeProvider", return_value=fixed_provider),
        ):
            generate_leinweber_offsets_flow.fn(
                run_parameters=BackfillLeinweberRunParameters(backfill_lookback_days=2)
            )

        assert checked_days == [datetime(2026, 3, 8), datetime(2026, 3, 9)]

    def _run_single_day_recording_durations(
        self, tmp_path, prefect_variables, variable_hours, **kwargs
    ):
        prefect_variables[LEINWEBER_VARIABLES.DURATION_HOURS_VARIABLE] = variable_hours
        repo = tmp_path / "repo"
        repo.mkdir()
        seen: dict[str, int] = {}

        def record_existing(datastore_finder, day, duration_hours, sensors):
            seen["existing"] = duration_hours
            return None

        def record_ready(datastore_finder, database, day, duration_hours, sensors):
            seen["science"] = duration_hours
            return True

        with (
            patch(
                f"{MODULE}.AppSettings",
                return_value=_patch_settings(tmp_path),
            ),
            patch(f"{MODULE}._existing_offsets", side_effect=record_existing),
            patch(f"{MODULE}._science_ready", side_effect=record_ready),
            patch(
                f"{MODULE}.generate_leinweber_offsets",
                return_value=[Path("offsets.csv")],
            ) as mock_generate,
        ):
            generate_leinweber_offsets_flow.fn(
                run_parameters=LeinweberByDateRunParameters(
                    start_date=DAY, force_regenerate=False
                ),
                matlab_repo=str(repo),
                **kwargs,
            )

        seen["cli"] = mock_generate.call_args.kwargs["duration_hours"]
        return seen

    def test_uses_duration_variable_when_duration_str_unset(
        self, tmp_path, prefect_variables
    ):
        seen = self._run_single_day_recording_durations(
            tmp_path, prefect_variables, variable_hours=48
        )

        assert seen == {"existing": 48, "science": 48, "cli": 48}

    def test_explicit_duration_str_overrides_duration_variable(
        self, tmp_path, prefect_variables
    ):
        seen = self._run_single_day_recording_durations(
            tmp_path, prefect_variables, variable_hours=48, duration_str="12h"
        )

        assert seen == {"existing": 12, "science": 12, "cli": 12}

    def test_requires_matlab_repo_when_a_day_needs_generating(self, tmp_path):
        with (
            patch(f"{MODULE}.AppSettings", return_value=_patch_settings(tmp_path)),
            patch(f"{MODULE}._existing_offsets", return_value=None),
            patch(f"{MODULE}._science_ready", return_value=True),
        ):
            with pytest.raises(TypeError, match="Unsupported matlab_repo type"):
                generate_leinweber_offsets_flow.fn(
                    run_parameters=LeinweberByDateRunParameters(
                        start_date=datetime(2026, 1, 30)
                    ),
                    matlab_repo=None,
                )

    def test_publishes_a_table_artifact_of_the_produced_offsets(
        self, tmp_path, _mock_offsets_artifact
    ):
        repo = tmp_path / "repo"
        repo.mkdir()

        with (
            patch(f"{MODULE}.AppSettings", return_value=_patch_settings(tmp_path)),
            patch(f"{MODULE}._existing_offsets", return_value=None),
            patch(f"{MODULE}._science_ready", return_value=True),
            patch(
                f"{MODULE}.generate_leinweber_offsets",
                return_value=[Path("offsets.csv")],
            ),
        ):
            result = generate_leinweber_offsets_flow.fn(
                run_parameters=LeinweberByDateRunParameters(
                    start_date=datetime(2026, 1, 30)
                ),
                matlab_repo=str(repo),
            )

        _mock_offsets_artifact.assert_called_once_with(result, datetime(2026, 1, 30))

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
                run_parameters=LeinweberByDateRunParameters(
                    start_date=datetime(2026, 1, 30),
                    end_date=datetime(2026, 2, 1),
                ),
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
                run_parameters=LeinweberByDateRunParameters(
                    start_date=datetime(2026, 1, 30)
                ),
                matlab_repo=str(repo),
                configure_midpoint=ConfigureMidpoint.USE_DATE_AS_IS,
            )

        assert mock_generate.call_args.kwargs["configure_midpoint"] == (
            ConfigureMidpoint.USE_DATE_AS_IS
        )

    @pytest.mark.parametrize(
        "run_parameters",
        [
            BackfillLeinweberRunParameters(backfill_lookback_days=1),
            LeinweberByDateRunParameters(
                start_date=datetime(2026, 1, 30), force_regenerate=False
            ),
        ],
        ids=["backfill-default", "by-date-opted-out"],
    )
    def test_skips_days_with_offsets_already_published(self, tmp_path, run_parameters):
        existing_path = Path("already-there.csv")

        with (
            patch(f"{MODULE}.AppSettings", return_value=_patch_settings(tmp_path)),
            patch(f"{MODULE}._existing_offsets", return_value=[existing_path]),
            patch(f"{MODULE}._science_ready") as mock_ready,
            patch(f"{MODULE}.generate_leinweber_offsets") as mock_generate,
        ):
            result = generate_leinweber_offsets_flow.fn(run_parameters=run_parameters)

        # Already-published short-circuits before the readiness check or any
        # MATLAB invocation.
        mock_ready.assert_not_called()
        mock_generate.assert_not_called()
        assert result == [existing_path]

    @pytest.mark.parametrize(
        "run_parameters",
        [
            LeinweberByDateRunParameters(start_date=datetime(2026, 1, 30)),
            BackfillLeinweberRunParameters(
                backfill_lookback_days=1, force_regenerate=True
            ),
        ],
        ids=["by-date-default", "backfill-opted-in"],
    )
    def test_force_regenerate_bypasses_the_already_published_skip(
        self, tmp_path, run_parameters
    ):
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
                run_parameters=run_parameters,
                matlab_repo=str(repo),
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
                run_parameters=LeinweberByDateRunParameters(
                    start_date=datetime(2026, 1, 30)
                ),
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
                run_parameters=LeinweberByDateRunParameters(
                    start_date=datetime(2026, 1, 30)
                ),
            )

        mock_generate.assert_not_called()
        # matlab_repo is never even resolved when there is nothing pending.
        mock_resolve.assert_not_called()
        assert result == []

    def test_proceeds_despite_incomplete_pivot_platform_data(self, tmp_path):
        # Pivot-platform incompleteness is advisory only - unlike _science_ready,
        # it must not skip the day.
        repo = tmp_path / "repo"
        repo.mkdir()

        with (
            patch(f"{MODULE}.AppSettings", return_value=_patch_settings(tmp_path)),
            patch(f"{MODULE}._existing_offsets", return_value=None),
            patch(f"{MODULE}._science_ready", return_value=True),
            patch(f"{MODULE}._pivot_platform_complete", return_value=False),
            patch(
                f"{MODULE}.generate_leinweber_offsets",
                return_value=[Path("offsets.csv")],
            ) as mock_generate,
        ):
            result = generate_leinweber_offsets_flow.fn(
                run_parameters=LeinweberByDateRunParameters(
                    start_date=datetime(2026, 1, 30)
                ),
                matlab_repo=str(repo),
            )

        mock_generate.assert_called_once()
        assert result == [Path("offsets.csv")]

    def test_mixed_batch_only_generates_pending_days(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        existing_path = Path("day1-already-there.csv")

        def fake_existing(datastore_finder, day, duration_hours, sensors):
            return [existing_path] if day == datetime(2026, 1, 30) else None

        def fake_ready(datastore_finder, database, day, duration_hours, sensors):
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
                run_parameters=LeinweberByDateRunParameters(
                    start_date=datetime(2026, 1, 30),
                    end_date=datetime(2026, 2, 1),
                    force_regenerate=False,
                ),
                matlab_repo=str(repo),
            )

        # Day 1 already published, day 2 ready and generated, day 3 not ready.
        mock_generate.assert_called_once()
        assert mock_generate.call_args.kwargs["date"] == datetime(2026, 1, 31)
        assert result == [existing_path, Path("generated.csv")]

    def test_default_run_parameters_have_correct_force_regenerate(self):
        assert BackfillLeinweberRunParameters().force_regenerate is False
        assert LeinweberByDateRunParameters(start_date=DAY).force_regenerate is True

    def test_default_run_parameters_have_correct_fail_on_missing_offsets(self):
        # Backfill tolerates a day not producing offsets; an explicit range does not.
        assert BackfillLeinweberRunParameters().fail_on_missing_offsets is False
        assert (
            LeinweberByDateRunParameters(start_date=DAY).fail_on_missing_offsets is True
        )

    def test_explicit_range_fails_when_offsets_are_missing(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()

        with (
            patch(f"{MODULE}.AppSettings", return_value=_patch_settings(tmp_path)),
            patch(f"{MODULE}._existing_offsets", return_value=None),
            patch(f"{MODULE}._science_ready", return_value=True),
            patch(
                f"{MODULE}.generate_leinweber_offsets",
                side_effect=NoOffsetsProducedError("no outputs"),
            ),
        ):
            with pytest.raises(NoOffsetsProducedError):
                generate_leinweber_offsets_flow.fn(
                    run_parameters=LeinweberByDateRunParameters(
                        start_date=datetime(2026, 1, 30)
                    ),
                    matlab_repo=str(repo),
                )

    def test_backfill_tolerates_missing_offsets_and_continues(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()

        def fake_generate(date, **kwargs):
            if date == datetime(2026, 1, 30):
                raise NoOffsetsProducedError("no outputs")
            return [Path("generated.csv")]

        with (
            patch(
                f"{MODULE}.AppSettings",
                return_value=_patch_settings(tmp_path),
            ),
            patch(f"{MODULE}.DatetimeProvider") as mock_provider,
            patch(f"{MODULE}._existing_offsets", return_value=None),
            patch(f"{MODULE}._science_ready", return_value=True),
            patch(
                f"{MODULE}.generate_leinweber_offsets", side_effect=fake_generate
            ) as mock_generate,
        ):
            mock_provider.return_value.yesterday.return_value = datetime(2026, 1, 31)
            result = generate_leinweber_offsets_flow.fn(
                run_parameters=BackfillLeinweberRunParameters(backfill_lookback_days=2),
                matlab_repo=str(repo),
            )

        # Both days attempted; the first's missing offsets didn't stop the second.
        assert mock_generate.call_count == 2
        assert result == [Path("generated.csv")]

    def test_explicit_range_can_opt_into_tolerating_missing_offsets(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()

        with (
            patch(f"{MODULE}.AppSettings", return_value=_patch_settings(tmp_path)),
            patch(f"{MODULE}._existing_offsets", return_value=None),
            patch(f"{MODULE}._science_ready", return_value=True),
            patch(
                f"{MODULE}.generate_leinweber_offsets",
                side_effect=NoOffsetsProducedError("no outputs"),
            ),
        ):
            result = generate_leinweber_offsets_flow.fn(
                run_parameters=LeinweberByDateRunParameters(
                    start_date=datetime(2026, 1, 30),
                    fail_on_missing_offsets=False,
                ),
                matlab_repo=str(repo),
            )

        assert result == []


class TestGenerateLeinweberOffsetsFlowSplitByDay:
    def _make_flow_run(self, name):
        fake = MagicMock()
        fake.name = name
        return fake

    def test_splits_range_into_per_day_deployment_runs(self):
        with (
            patch(
                f"{MODULE}.run_deployment",
                side_effect=lambda **kwargs: self._make_flow_run(
                    str(kwargs["parameters"]["run_parameters"].start_date)
                ),
            ) as mock_run_deployment,
            patch(f"{MODULE}.generate_leinweber_offsets") as mock_generate,
        ):
            result = generate_leinweber_offsets_flow.fn(
                run_parameters=LeinweberByDateRunParameters(
                    start_date=datetime(2025, 1, 1),
                    end_date=datetime(2025, 1, 3),
                ),
                matlab_repo="some-repo",
                split_by_day=True,
            )

        assert mock_generate.call_count == 0
        assert mock_run_deployment.call_count == 3
        for day, call in zip(
            [datetime(2025, 1, 1), datetime(2025, 1, 2), datetime(2025, 1, 3)],
            mock_run_deployment.call_args_list,
        ):
            assert call.kwargs["name"] == (
                f"{PREFECT_CONSTANTS.FLOW_NAMES.GENERATE_LEINWEBER_OFFSETS}/"
                f"{PREFECT_CONSTANTS.DEPLOYMENT_NAMES.GENERATE_LEINWEBER_OFFSETS}"
            )
            run_parameters = call.kwargs["parameters"]["run_parameters"]
            assert run_parameters.start_date == day
            assert run_parameters.end_date == day
            # Explicit ranges default to failing on missing offsets; that
            # carries over to each per-day sub-run.
            assert run_parameters.fail_on_missing_offsets is True
            assert call.kwargs["parameters"]["split_by_day"] is False
            assert call.kwargs["parameters"]["configure_midpoint"] == DEFAULT_MIDPOINT
            # Explicit ranges default to regenerating; that also carries over.
            assert run_parameters.force_regenerate is True
            assert "force_regenerate" not in call.kwargs["parameters"]
            assert (
                call.kwargs["parameters"]["allow_pivot_platform_extrapolation"] is False
            )
        assert len(result) == 3

    def test_backfill_split_carries_backfill_defaults_to_each_day(self, tmp_path):
        # A day split off from a backfill sweep must stay tolerant of missing
        # offsets and keep skipping already-published days, even though it
        # becomes an ordinary LeinweberByDateRunParameters (whose own defaults
        # are the opposite).
        fixed_provider = DatetimeProvider(fixed_now=datetime(2026, 3, 10))

        with (
            patch(f"{MODULE}.AppSettings", return_value=_patch_settings(tmp_path)),
            patch(f"{MODULE}.DatetimeProvider", return_value=fixed_provider),
            patch(
                f"{MODULE}.run_deployment",
                side_effect=lambda **kwargs: self._make_flow_run(
                    str(kwargs["parameters"]["run_parameters"].start_date)
                ),
            ) as mock_run_deployment,
            patch(f"{MODULE}.generate_leinweber_offsets") as mock_generate,
        ):
            generate_leinweber_offsets_flow.fn(
                run_parameters=BackfillLeinweberRunParameters(backfill_lookback_days=2),
                matlab_repo="some-repo",
                split_by_day=True,
            )

        assert mock_generate.call_count == 0
        assert mock_run_deployment.call_count == 2
        for call in mock_run_deployment.call_args_list:
            run_parameters = call.kwargs["parameters"]["run_parameters"]
            assert run_parameters.fail_on_missing_offsets is False
            assert run_parameters.force_regenerate is False

    def test_single_day_does_not_split(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()

        with (
            patch(f"{MODULE}.run_deployment") as mock_run_deployment,
            patch(f"{MODULE}.AppSettings", return_value=_patch_settings(tmp_path)),
            patch(f"{MODULE}._existing_offsets", return_value=None),
            patch(f"{MODULE}._science_ready", return_value=True),
            patch(f"{MODULE}._create_offsets_table_artifact"),
            patch(
                f"{MODULE}.generate_leinweber_offsets",
                return_value=[Path("offsets.csv")],
            ) as mock_generate,
        ):
            generate_leinweber_offsets_flow.fn(
                run_parameters=LeinweberByDateRunParameters(
                    start_date=datetime(2026, 1, 30)
                ),
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
        assert (
            _science_ready(finder, None, DAY, 24, [Sensor.MAGO, Sensor.MAGI]) is False
        )

    def test_false_when_only_target_day_present_not_neighbours(self, tmp_path):
        self._science_file(tmp_path, "mago", DAY)
        self._science_file(tmp_path, "magi", DAY)
        finder = FileFinder(tmp_path)
        assert (
            _science_ready(finder, None, DAY, 24, [Sensor.MAGO, Sensor.MAGI]) is False
        )

    def test_false_when_only_one_sensor_present_for_all_touched_days(self, tmp_path):
        for day in self._TOUCHED_DAYS:
            self._science_file(tmp_path, "mago", day)
        finder = FileFinder(tmp_path)
        assert (
            _science_ready(finder, None, DAY, 24, [Sensor.MAGO, Sensor.MAGI]) is False
        )

    def test_true_when_all_touched_days_present_for_all_sensors(self, tmp_path):
        for day in self._TOUCHED_DAYS:
            self._science_file(tmp_path, "mago", day)
            self._science_file(tmp_path, "magi", day)
        finder = FileFinder(tmp_path)
        assert _science_ready(finder, None, DAY, 24, [Sensor.MAGO, Sensor.MAGI]) is True

    def test_true_when_only_requesting_the_sensor_present(self, tmp_path):
        for day in self._TOUCHED_DAYS:
            self._science_file(tmp_path, "mago", day)
        finder = FileFinder(tmp_path)
        assert _science_ready(finder, None, DAY, 24, [Sensor.MAGO]) is True

    # A range that comfortably covers every window these tests check (DAY=
    # 2026-01-30 with duration_hours=24 spans 2026-01-29 23:59..2026-01-31 00:01).
    _FULL_COVERAGE: ClassVar[dict] = {
        "first_timestamp": datetime(2026, 1, 29),
        "last_timestamp": datetime(2026, 2, 1),
    }

    def test_false_when_database_has_a_gap_overlapping_the_window(self, tmp_path):
        for day in self._TOUCHED_DAYS:
            self._science_file(tmp_path, "mago", day)
        finder = FileFinder(tmp_path)

        mock_file = MagicMock(id=1)
        mock_analysis = MagicMock(
            gaps=[
                {
                    "start_time": "2026-01-30T10:00:00",
                    "end_time": "2026-01-30T10:05:00",
                    "duration_seconds": 300,
                }
            ],
            has_missing_data=True,
            has_bad_data=True,
            **self._FULL_COVERAGE,
        )
        database = MagicMock()
        database.get_files.return_value = [mock_file]
        database.get_file_analysis_by_file_id.return_value = mock_analysis

        assert _science_ready(finder, database, DAY, 24, [Sensor.MAGO]) is False

    def test_true_when_only_missing_or_bad_data_flags_are_set(self, tmp_path):
        # has_missing_data/has_bad_data are unreliable and must not gate readiness
        # on their own - only an actual gap overlapping the window should.
        for day in self._TOUCHED_DAYS:
            self._science_file(tmp_path, "mago", day)
        finder = FileFinder(tmp_path)

        mock_file = MagicMock(id=1)
        mock_analysis = MagicMock(
            gaps=[],
            has_missing_data=True,
            has_bad_data=True,
            **self._FULL_COVERAGE,
        )
        database = MagicMock()
        database.get_files.return_value = [mock_file]
        database.get_file_analysis_by_file_id.return_value = mock_analysis

        assert _science_ready(finder, database, DAY, 24, [Sensor.MAGO]) is True

    def test_true_when_a_recorded_gap_does_not_overlap_the_window(self, tmp_path):
        for day in self._TOUCHED_DAYS:
            self._science_file(tmp_path, "mago", day)
        finder = FileFinder(tmp_path)

        mock_file = MagicMock(id=1)
        mock_analysis = MagicMock(
            gaps=[
                {
                    # Well outside the 24h window around DAY (2026-01-30).
                    "start_time": "2026-02-15T10:00:00",
                    "end_time": "2026-02-15T10:05:00",
                    "duration_seconds": 300,
                }
            ],
            has_missing_data=False,
            has_bad_data=False,
            **self._FULL_COVERAGE,
        )
        database = MagicMock()
        database.get_files.return_value = [mock_file]
        database.get_file_analysis_by_file_id.return_value = mock_analysis

        assert _science_ready(finder, database, DAY, 24, [Sensor.MAGO]) is True

    def test_false_when_file_data_ends_before_the_window_does(self, tmp_path):
        # The file's own last record is well before the window's end, with no
        # later record to diff against - a trailing gap `gaps` can't see.
        for day in self._TOUCHED_DAYS:
            self._science_file(tmp_path, "mago", day)
        finder = FileFinder(tmp_path)

        mock_file = MagicMock(id=1)
        mock_analysis = MagicMock(
            gaps=[],
            has_missing_data=False,
            has_bad_data=False,
            first_timestamp=datetime(2026, 1, 29),
            last_timestamp=datetime(2026, 1, 30, 18, 0),
        )
        database = MagicMock()
        database.get_files.return_value = [mock_file]
        database.get_file_analysis_by_file_id.return_value = mock_analysis

        assert _science_ready(finder, database, DAY, 24, [Sensor.MAGO]) is False

    def test_true_when_database_has_no_analysis_yet_for_the_file(self, tmp_path):
        for day in self._TOUCHED_DAYS:
            self._science_file(tmp_path, "mago", day)
        finder = FileFinder(tmp_path)

        database = MagicMock()
        database.get_files.return_value = []

        assert _science_ready(finder, database, DAY, 24, [Sensor.MAGO]) is True


class TestWindowBounds:
    def test_matches_the_rounded_window_days_span(self):
        window_start, window_end = _window_bounds(DAY, 24)
        assert window_start == datetime(2026, 1, 29, 23, 59)
        assert window_end == datetime(2026, 1, 31, 0, 1)


class TestFileHasGapInWindow:
    WINDOW_START = datetime(2026, 1, 29, 23, 59)
    WINDOW_END = datetime(2026, 1, 31, 0, 1)

    # Comfortably covers WINDOW_START..WINDOW_END, so tests using this isolate
    # the behaviour under test (gaps list, or lack of a database/analysis) from
    # the first/last-timestamp edge-coverage check.
    _FULL_COVERAGE: ClassVar[dict] = {
        "first_timestamp": datetime(2026, 1, 29),
        "last_timestamp": datetime(2026, 2, 1),
    }

    def test_false_when_no_database(self):
        assert (
            _file_has_gap_in_window(
                None, "some-file.cdf", self.WINDOW_START, self.WINDOW_END
            )
            is False
        )

    def test_false_when_file_not_found(self):
        database = MagicMock()
        database.get_files.return_value = []
        assert (
            _file_has_gap_in_window(
                database, "some-file.cdf", self.WINDOW_START, self.WINDOW_END
            )
            is False
        )

    def test_false_when_no_analysis_record(self):
        database = MagicMock()
        database.get_files.return_value = [MagicMock(id=1)]
        database.get_file_analysis_by_file_id.return_value = None
        assert (
            _file_has_gap_in_window(
                database, "some-file.cdf", self.WINDOW_START, self.WINDOW_END
            )
            is False
        )

    def test_false_when_no_gaps_recorded_and_coverage_spans_the_window(self):
        database = MagicMock()
        database.get_files.return_value = [MagicMock(id=1)]
        database.get_file_analysis_by_file_id.return_value = MagicMock(
            gaps=[], **self._FULL_COVERAGE
        )
        assert (
            _file_has_gap_in_window(
                database, "some-file.cdf", self.WINDOW_START, self.WINDOW_END
            )
            is False
        )

    def test_false_when_recorded_gap_is_outside_the_window(self):
        database = MagicMock()
        database.get_files.return_value = [MagicMock(id=1)]
        database.get_file_analysis_by_file_id.return_value = MagicMock(
            gaps=[
                {
                    "start_time": "2026-02-15T10:00:00",
                    "end_time": "2026-02-15T10:05:00",
                    "duration_seconds": 300,
                }
            ],
            **self._FULL_COVERAGE,
        )
        assert (
            _file_has_gap_in_window(
                database, "some-file.cdf", self.WINDOW_START, self.WINDOW_END
            )
            is False
        )

    def test_true_when_recorded_gap_overlaps_the_window(self):
        database = MagicMock()
        database.get_files.return_value = [MagicMock(id=1)]
        database.get_file_analysis_by_file_id.return_value = MagicMock(
            gaps=[
                {
                    "start_time": "2026-01-30T10:00:00",
                    "end_time": "2026-01-30T10:05:00",
                    "duration_seconds": 300,
                }
            ],
            **self._FULL_COVERAGE,
        )
        assert (
            _file_has_gap_in_window(
                database, "some-file.cdf", self.WINDOW_START, self.WINDOW_END
            )
            is True
        )

    def test_true_when_recorded_gap_only_partially_overlaps_the_window(self):
        database = MagicMock()
        database.get_files.return_value = [MagicMock(id=1)]
        database.get_file_analysis_by_file_id.return_value = MagicMock(
            gaps=[
                {
                    # Starts before the window and ends just inside it.
                    "start_time": "2026-01-29T20:00:00",
                    "end_time": "2026-01-30T00:00:01",
                    "duration_seconds": 14401,
                }
            ],
            **self._FULL_COVERAGE,
        )
        assert (
            _file_has_gap_in_window(
                database, "some-file.cdf", self.WINDOW_START, self.WINDOW_END
            )
            is True
        )

    def test_true_when_no_timestamps_recorded_at_all(self):
        # Analysis ran but found 0-1 usable timestamps - coverage can't be
        # confirmed, so treat as incomplete rather than assuming the best.
        database = MagicMock()
        database.get_files.return_value = [MagicMock(id=1)]
        database.get_file_analysis_by_file_id.return_value = MagicMock(
            gaps=[], first_timestamp=None, last_timestamp=None
        )
        assert (
            _file_has_gap_in_window(
                database, "some-file.cdf", self.WINDOW_START, self.WINDOW_END
            )
            is True
        )

    def test_true_when_data_starts_after_the_window_start(self):
        # The file's first record is after the window needs coverage from - a
        # leading gap `gaps` can't see, since there's no earlier record to diff
        # against.
        database = MagicMock()
        database.get_files.return_value = [MagicMock(id=1)]
        database.get_file_analysis_by_file_id.return_value = MagicMock(
            gaps=[],
            first_timestamp=datetime(2026, 1, 30, 6, 0),
            last_timestamp=datetime(2026, 2, 1),
        )
        assert (
            _file_has_gap_in_window(
                database, "some-file.cdf", self.WINDOW_START, self.WINDOW_END
            )
            is True
        )

    def test_true_when_data_ends_before_the_window_end(self):
        # The file's last record is before the window needs coverage until - a
        # trailing gap at the end of the day/window `gaps` can't see, since
        # there's no later record to diff against.
        database = MagicMock()
        database.get_files.return_value = [MagicMock(id=1)]
        database.get_file_analysis_by_file_id.return_value = MagicMock(
            gaps=[],
            first_timestamp=datetime(2026, 1, 29),
            last_timestamp=datetime(2026, 1, 30, 18, 0),
        )
        assert (
            _file_has_gap_in_window(
                database, "some-file.cdf", self.WINDOW_START, self.WINDOW_END
            )
            is True
        )


class TestPivotPlatformComplete:
    def _pivot_file(self, datastore: Path, day: datetime) -> None:
        folder = (
            datastore
            / "hk"
            / "lo"
            / "l1"
            / "pivot-platform-angle"
            / f"{day:%Y}"
            / f"{day:%m}"
        )
        folder.mkdir(parents=True, exist_ok=True)
        (folder / f"imap_lo_l1_pivot-platform-angle_{day:%Y%m%d}_v001.csv").write_text(
            "x"
        )

    _TOUCHED_DAYS = (
        datetime(2026, 1, 29),
        datetime(2026, 1, 30),
        datetime(2026, 1, 31),
    )

    def test_false_when_datastore_empty(self, tmp_path):
        finder = FileFinder(tmp_path)
        assert _pivot_platform_complete(finder, None, DAY, 24) is False

    def test_false_when_only_some_touched_days_present(self, tmp_path):
        self._pivot_file(tmp_path, DAY)
        finder = FileFinder(tmp_path)
        assert _pivot_platform_complete(finder, None, DAY, 24) is False

    def test_true_when_all_touched_days_present_and_no_database(self, tmp_path):
        for day in self._TOUCHED_DAYS:
            self._pivot_file(tmp_path, day)
        finder = FileFinder(tmp_path)
        assert _pivot_platform_complete(finder, None, DAY, 24) is True

    # Comfortably covers the 24h window around DAY (2026-01-29 23:59..
    # 2026-01-31 00:01), isolating the behaviour under test from the
    # first/last-timestamp edge-coverage check.
    _FULL_COVERAGE: ClassVar[dict] = {
        "first_timestamp": datetime(2026, 1, 29),
        "last_timestamp": datetime(2026, 2, 1),
    }

    def test_false_when_database_has_a_gap_overlapping_the_window(self, tmp_path):
        for day in self._TOUCHED_DAYS:
            self._pivot_file(tmp_path, day)
        finder = FileFinder(tmp_path)

        database = MagicMock()
        database.get_files.return_value = [MagicMock(id=1)]
        database.get_file_analysis_by_file_id.return_value = MagicMock(
            gaps=[
                {
                    "start_time": "2026-01-30T10:00:00",
                    "end_time": "2026-01-30T10:05:00",
                    "duration_seconds": 300,
                }
            ],
            **self._FULL_COVERAGE,
        )

        assert _pivot_platform_complete(finder, database, DAY, 24) is False

    def test_true_when_only_missing_or_bad_data_flags_are_set(self, tmp_path):
        for day in self._TOUCHED_DAYS:
            self._pivot_file(tmp_path, day)
        finder = FileFinder(tmp_path)

        database = MagicMock()
        database.get_files.return_value = [MagicMock(id=1)]
        database.get_file_analysis_by_file_id.return_value = MagicMock(
            gaps=[],
            has_missing_data=True,
            has_bad_data=True,
            **self._FULL_COVERAGE,
        )

        assert _pivot_platform_complete(finder, database, DAY, 24) is True

    def test_false_when_pivot_file_data_ends_before_the_window_does(self, tmp_path):
        for day in self._TOUCHED_DAYS:
            self._pivot_file(tmp_path, day)
        finder = FileFinder(tmp_path)

        database = MagicMock()
        database.get_files.return_value = [MagicMock(id=1)]
        database.get_file_analysis_by_file_id.return_value = MagicMock(
            gaps=[],
            first_timestamp=datetime(2026, 1, 29),
            last_timestamp=datetime(2026, 1, 30, 18, 0),
        )

        assert _pivot_platform_complete(finder, database, DAY, 24) is False


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


class TestCreateOffsetsTableArtifact:
    # Mirrors the columns generate_raw_leinweber_offset.m's empty_offset_table
    # writes - one row per sensor/day CSV.
    _HEADER = (
        "Date,MidPoint,StartWindow,EndWindow,OffsetX,OffsetY,OffsetZ,"
        "Dynamic MCS,Percent,VerifyResidual,Sensor"
    )

    def _offsets_csv(
        self, tmp_path: Path, sensor: str, verify_residual: str = "0.0162"
    ) -> Path:
        path = tmp_path / f"imap_mag_{sensor}-raw-leinweber-offsets-24hr_20260130.csv"
        path.write_text(
            self._HEADER + "\n"
            "30-Jan-2026 22:35:00.000,30-Jan-2026 22:35:00.000,"
            "30-Jan-2026 10:35:00.000,31-Jan-2026 10:35:00.000,"
            f"-0.015994,6.3428,0.23549,0.88231,42.5,{verify_residual},{sensor}\n"
        )
        return path

    def test_does_not_create_an_artifact_when_nothing_was_produced(self):
        with patch(f"{MODULE}.create_table_artifact") as mock_create:
            _create_offsets_table_artifact([], datetime(2026, 1, 30))

        mock_create.assert_not_called()

    def test_key_is_scoped_to_the_start_date(self, tmp_path):
        csv_path = self._offsets_csv(tmp_path, "mago")

        with patch(f"{MODULE}.create_table_artifact") as mock_create:
            _create_offsets_table_artifact([csv_path], datetime(2026, 1, 30))

        assert mock_create.call_args.kwargs["key"] == "2026-01-30-leinweber-offsets"

    def test_publishes_one_row_per_file_with_the_expected_values(self, tmp_path):
        mago_csv = self._offsets_csv(tmp_path, "mago")
        magi_csv = self._offsets_csv(tmp_path, "magi", verify_residual="")

        with patch(f"{MODULE}.create_table_artifact") as mock_create:
            _create_offsets_table_artifact([mago_csv, magi_csv], datetime(2026, 1, 30))

        mock_create.assert_called_once()
        table = mock_create.call_args.kwargs["table"]
        assert len(table) == 2

        mago_row = table[0]
        assert mago_row["Sensor"] == "mago"
        assert mago_row["Offset X (nT)"] == -0.016
        assert mago_row["Offset Y (nT)"] == 6.3428
        assert mago_row["Offset Z (nT)"] == 0.2355
        assert mago_row["Dynamic MCS"] == 0.8823
        assert mago_row["Percent"] == 42.5
        assert mago_row["Verify Residual (nT)"] == 0.0162
        assert mago_row["File"] == mago_csv.name

        # An empty VerifyResidual (verify=False) becomes None, not NaN, so the
        # artifact JSON-encodes cleanly.
        assert table[1]["Verify Residual (nT)"] is None
