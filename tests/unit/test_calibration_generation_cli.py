"""Tests for the imap-cal-gen CLI in calibration_generation.main."""

import numpy as np
import pytest
from spacepy import pycdf
from typer.testing import CliRunner

from calibration_generation.main import (
    DEFAULT_IALIRT_GRADIOMETER_FACTOR,
    DEFAULT_NUMBER_OF_SPINS,
    DEFAULT_QUALITY_FLAG_THRESHOLD,
    DEFAULT_SPIN_AVERAGE_FACTOR,
    app,
)
from calibration_generation.matrices import (
    LATEST_MATRIX_VERSION,
    MatrixSet,
    get_frame_transforms,
)

runner = CliRunner()

L2_ARGS = ["l2", "--version", "1", "--valid-start-date", "2026-01-02"]
IALIRT_ARGS = [
    "ialirt",
    "--version",
    "1",
    "--valid-start-date",
    "2026-01-02",
    "--gradiometer-factor",
    "0.35",
]
L1D_ARGS = [
    "l1d",
    "--version",
    "1",
    "--valid-start-date",
    "2026-01-02",
    "--gradiometer-factor",
    "0.35",
    "--spin-average-factor",
    "1.0",
    "--number-of-spins",
    "4",
    "--quality-flag-threshold",
    "2.5",
]


def invoke(args: list[str], folder, *extra: str, **kwargs):
    """Run a generation command non-interactively.

    Every prompted option has to be supplied on the command line, otherwise the
    command waits for input that the runner has no way of giving it. Tests that
    are about the prompts themselves call the runner directly.
    """
    matrices = [] if "--matrices" in extra else ["--matrices", "latest"]
    return runner.invoke(
        app,
        [*args, *matrices, "--output-folder", str(folder), "--yes", *extra],
        **kwargs,
    )


class TestL2Command:
    def test_writes_a_file(self, tmp_path):
        result = invoke(L2_ARGS, tmp_path)

        assert result.exit_code == 0, result.output
        assert (tmp_path / "imap_mag_l2-calibration_20260102_v001.cdf").exists()

    def test_writes_the_matrices_it_was_given(self, tmp_path):
        invoke(L2_ARGS, tmp_path, "--matrices", f"v{LATEST_MATRIX_VERSION}")

        expected, _ = get_frame_transforms(MatrixSet.LATEST)
        path = tmp_path / "imap_mag_l2-calibration_20260102_v001.cdf"
        with pycdf.CDF(str(path), readonly=True) as cdf:
            np.testing.assert_allclose(np.asarray(cdf["URFTOORFO"][...]), expected)

    def test_matrices_can_be_chosen(self, tmp_path):
        result = invoke(L2_ARGS, tmp_path, "--matrices", "identity")

        assert result.exit_code == 0, result.output
        path = tmp_path / "imap_mag_l2-calibration_20260102_v001.cdf"
        with pycdf.CDF(str(path), readonly=True) as cdf:
            np.testing.assert_array_equal(
                np.asarray(cdf["URFTOORFO"][...])[:, :, 0], np.eye(3)
            )

    def test_unknown_matrix_set_is_rejected(self, tmp_path):
        result = invoke(L2_ARGS, tmp_path, "--matrices", "v42")

        assert result.exit_code != 0

    def test_compact_dates_are_accepted(self, tmp_path):
        result = invoke(
            ["l2", "--version", "1", "--valid-start-date", "20260102"], tmp_path
        )

        assert result.exit_code == 0, result.output

    def test_existing_file_is_not_overwritten(self, tmp_path):
        invoke(L2_ARGS, tmp_path)

        result = invoke(L2_ARGS, tmp_path)

        assert result.exit_code != 0
        assert isinstance(result.exception, FileExistsError)


class TestMatricesPrompt:
    def _invoke_with_input(self, tmp_path, text: str):
        return runner.invoke(
            app,
            [*L2_ARGS, "--output-folder", str(tmp_path), "--yes"],
            input=text,
        )

    def test_matrices_are_asked_for_when_not_given(self, tmp_path):
        result = self._invoke_with_input(tmp_path, "identity\n")

        assert result.exit_code == 0, result.output
        assert "Matrix set" in result.output
        path = tmp_path / "imap_mag_l2-calibration_20260102_v001.cdf"
        with pycdf.CDF(str(path), readonly=True) as cdf:
            np.testing.assert_array_equal(
                np.asarray(cdf["URFTOORFO"][...])[:, :, 0], np.eye(3)
            )

    def test_the_choices_and_default_are_offered(self, tmp_path):
        result = self._invoke_with_input(tmp_path, "\n")

        for choice in MatrixSet:
            assert choice.value in result.output
        assert f"[{MatrixSet.LATEST.value}]" in result.output

    def test_accepting_the_default_uses_the_latest_matrices(self, tmp_path):
        result = self._invoke_with_input(tmp_path, "\n")

        assert result.exit_code == 0, result.output
        expected, _ = get_frame_transforms(MatrixSet.LATEST)
        path = tmp_path / "imap_mag_l2-calibration_20260102_v001.cdf"
        with pycdf.CDF(str(path), readonly=True) as cdf:
            np.testing.assert_allclose(np.asarray(cdf["URFTOORFO"][...]), expected)

    def test_an_invalid_answer_is_asked_again(self, tmp_path):
        result = self._invoke_with_input(tmp_path, "v42\nidentity\n")

        assert result.exit_code == 0, result.output
        assert result.output.count("Matrix set") == 2

    def test_passing_the_option_skips_the_prompt(self, tmp_path):
        result = invoke(L2_ARGS, tmp_path, "--matrices", "identity")

        assert result.exit_code == 0, result.output
        assert "Matrix set" not in result.output


class TestOffsetOptions:
    def test_inline_offsets(self, tmp_path):
        result = invoke(
            IALIRT_ARGS, tmp_path, "--mago", "-11.2,0.4,3.1", "--magi", "-21.9,1,4.2"
        )

        assert result.exit_code == 0, result.output
        path = tmp_path / "imap_mag_ialirt-calibration_20260102_v001.cdf"
        with pycdf.CDF(str(path), readonly=True) as cdf:
            np.testing.assert_allclose(
                np.asarray(cdf["offsets"][...])[0, 0], [-11.2, 0.4, 3.1]
            )

    def test_zero_offsets_flag(self, tmp_path):
        result = invoke(IALIRT_ARGS, tmp_path, "--zero-offsets")

        assert result.exit_code == 0, result.output
        path = tmp_path / "imap_mag_ialirt-calibration_20260102_v001.cdf"
        with pycdf.CDF(str(path), readonly=True) as cdf:
            assert not np.asarray(cdf["offsets"][...]).any()

    def test_offsets_file(self, tmp_path):
        offsets_file = tmp_path / "offsets.yaml"
        offsets_file.write_text("MAGo: [1, 2, 3]\nMAGi: [4, 5, 6]\n")

        result = invoke(IALIRT_ARGS, tmp_path, "--offsets-file", str(offsets_file))

        assert result.exit_code == 0, result.output
        path = tmp_path / "imap_mag_ialirt-calibration_20260102_v001.cdf"
        with pycdf.CDF(str(path), readonly=True) as cdf:
            np.testing.assert_allclose(
                np.asarray(cdf["offsets"][...])[1, 0], [4.0, 5.0, 6.0]
            )

    def test_per_range_inline_offsets(self, tmp_path):
        result = invoke(
            L1D_ARGS,
            tmp_path,
            "--mago",
            "1,1,1; 2,2,2; 3,3,3; 4,4,4",
            "--magi",
            "9,9,9",
        )

        assert result.exit_code == 0, result.output
        path = tmp_path / "imap_mag_l1d-calibration_20260102_v001.cdf"
        with pycdf.CDF(str(path), readonly=True) as cdf:
            np.testing.assert_allclose(
                np.asarray(cdf["offsets"][...])[0, 3], [4.0, 4.0, 4.0]
            )

    def test_bad_offsets_exit_with_a_usage_error_and_no_file(self, tmp_path):
        result = invoke(IALIRT_ARGS, tmp_path, "--mago", "not,a,vector")

        assert result.exit_code == 2
        assert "not a set of numbers" in result.output
        assert not list(tmp_path.glob("*.cdf"))

    def test_conflicting_offset_options_are_rejected(self, tmp_path):
        offsets_file = tmp_path / "offsets.yaml"
        offsets_file.write_text("MAGo: [1, 2, 3]\nMAGi: [4, 5, 6]\n")

        result = invoke(
            IALIRT_ARGS,
            tmp_path,
            "--mago",
            "1,2,3",
            "--offsets-file",
            str(offsets_file),
        )

        assert result.exit_code == 2
        assert "not both" in result.output

    def test_missing_offsets_file_is_rejected(self, tmp_path):
        result = invoke(IALIRT_ARGS, tmp_path, "--offsets-file", "nope.yaml")

        assert result.exit_code != 0

    def test_offsets_are_shown_before_writing(self, tmp_path):
        result = invoke(IALIRT_ARGS, tmp_path, "--mago", "1,2,3", "--magi", "4,5,6")

        assert "MAGo" in result.output
        assert "Offsets" in result.output

    def test_suspicious_magnitudes_are_called_out(self, tmp_path):
        result = invoke(IALIRT_ARGS, tmp_path, "--mago", "50,0,0", "--magi", "1,0,0")

        assert result.exit_code == 0, result.output
        assert "not smaller than" in result.output


class TestL1dCommand:
    def test_writes_the_spin_and_quality_parameters(self, tmp_path):
        result = invoke(L1D_ARGS, tmp_path, "--zero-offsets")

        assert result.exit_code == 0, result.output
        path = tmp_path / "imap_mag_l1d-calibration_20260102_v001.cdf"
        with pycdf.CDF(str(path), readonly=True) as cdf:
            assert cdf["number_of_spins"][...] == 4
            assert cdf["quality_flag_threshold"][...] == pytest.approx(2.5)

    def test_zero_spins_is_rejected(self, tmp_path):
        args = list(L1D_ARGS)
        args[args.index("--number-of-spins") + 1] = "0"

        result = invoke(args, tmp_path, "--zero-offsets")

        assert result.exit_code != 0


class TestParameterDefaults:
    """Parameters with a standard value are prompted with it already filled in."""

    def test_accepting_the_ialirt_gradiometer_prompt_uses_the_default(self, tmp_path):
        result = runner.invoke(
            app,
            [
                "ialirt",
                "--version",
                "1",
                "--valid-start-date",
                "2026-01-02",
                "--zero-offsets",
                "--matrices",
                "latest",
                "--output-folder",
                str(tmp_path),
                "--yes",
            ],
            input="\n",
        )

        assert result.exit_code == 0, result.output
        path = tmp_path / "imap_mag_ialirt-calibration_20260102_v001.cdf"
        with pycdf.CDF(str(path), readonly=True) as cdf:
            np.testing.assert_allclose(
                np.asarray(cdf["gradiometer_factor"][...]),
                DEFAULT_IALIRT_GRADIOMETER_FACTOR * np.eye(3),
            )

    def test_accepting_the_l1d_prompts_uses_the_defaults(self, tmp_path):
        result = runner.invoke(
            app,
            [
                "l1d",
                "--version",
                "1",
                "--valid-start-date",
                "2026-01-02",
                "--gradiometer-factor",
                "0.35",
                "--zero-offsets",
                "--matrices",
                "latest",
                "--output-folder",
                str(tmp_path),
                "--yes",
            ],
            # Spin average factor, number of spins, quality flag threshold.
            input="\n\n\n",
        )

        assert result.exit_code == 0, result.output
        path = tmp_path / "imap_mag_l1d-calibration_20260102_v001.cdf"
        with pycdf.CDF(str(path), readonly=True) as cdf:
            assert cdf["spin_average_application_factor"][...] == pytest.approx(
                DEFAULT_SPIN_AVERAGE_FACTOR
            )
            assert cdf["number_of_spins"][...] == DEFAULT_NUMBER_OF_SPINS
            assert cdf["quality_flag_threshold"][...] == pytest.approx(
                DEFAULT_QUALITY_FLAG_THRESHOLD
            )

    def test_l1d_defaults_are_offered_at_the_prompts(self, tmp_path):
        result = runner.invoke(
            app,
            [
                "l1d",
                "--version",
                "1",
                "--valid-start-date",
                "2026-01-02",
                "--gradiometer-factor",
                "0.35",
                "--zero-offsets",
                "--matrices",
                "latest",
                "--output-folder",
                str(tmp_path),
                "--dry-run",
            ],
            input="\n\n\n",
        )

        assert result.exit_code == 0, result.output
        assert f"[{DEFAULT_SPIN_AVERAGE_FACTOR}]" in result.output
        assert f"[{DEFAULT_NUMBER_OF_SPINS}]" in result.output
        assert f"[{DEFAULT_QUALITY_FLAG_THRESHOLD}]" in result.output

    def test_defaults_are_offered_at_the_prompt(self, tmp_path):
        result = runner.invoke(
            app,
            [
                "ialirt",
                "--version",
                "1",
                "--valid-start-date",
                "2026-01-02",
                "--zero-offsets",
                "--output-folder",
                str(tmp_path),
                "--dry-run",
            ],
            input="\n\n",
        )

        assert result.exit_code == 0, result.output
        assert f"[{DEFAULT_IALIRT_GRADIOMETER_FACTOR}]" in result.output

    def test_given_values_win_over_the_defaults(self, tmp_path):
        result = invoke(L1D_ARGS, tmp_path, "--zero-offsets")

        assert result.exit_code == 0, result.output
        path = tmp_path / "imap_mag_l1d-calibration_20260102_v001.cdf"
        with pycdf.CDF(str(path), readonly=True) as cdf:
            assert cdf["number_of_spins"][...] == 4
            assert cdf["quality_flag_threshold"][...] == pytest.approx(2.5)

    def test_l1d_gradiometer_factor_has_no_default_and_is_asked_for(self, tmp_path):
        """Only I-ALiRT has a standard gradiometer factor."""
        result = runner.invoke(
            app,
            [
                "l1d",
                "--version",
                "1",
                "--valid-start-date",
                "2026-01-02",
                "--zero-offsets",
                "--matrices",
                "latest",
                "--output-folder",
                str(tmp_path),
                "--dry-run",
            ],
            input="0.35\n\n\n\n",
        )

        assert result.exit_code == 0, result.output
        assert "Gradiometer factor:" in result.output


class TestConfirmation:
    def _invoke_answering(self, tmp_path, confirmation: str):
        """Accept the default matrices, then answer the confirmation."""
        return runner.invoke(
            app,
            [*L2_ARGS, "--output-folder", str(tmp_path)],
            input=f"\n{confirmation}\n",
        )

    def test_declining_writes_nothing(self, tmp_path):
        result = self._invoke_answering(tmp_path, "n")

        assert result.exit_code != 0
        assert "Nothing written" in result.output
        assert not list(tmp_path.glob("*.cdf"))

    def test_accepting_writes_the_file(self, tmp_path):
        result = self._invoke_answering(tmp_path, "y")

        assert result.exit_code == 0, result.output
        assert list(tmp_path.glob("*.cdf"))

    def test_dry_run_shows_the_summary_without_writing(self, tmp_path):
        result = invoke(L2_ARGS, tmp_path, "--dry-run")

        assert result.exit_code == 0, result.output
        assert "nothing written" in result.output.lower()
        assert not list(tmp_path.glob("*.cdf"))

    def test_dry_run_shows_offsets(self, tmp_path):
        result = invoke(
            IALIRT_ARGS, tmp_path, "--dry-run", "--mago", "1,2,3", "--magi", "4,5,6"
        )

        assert result.exit_code == 0, result.output
        assert "MAGi" in result.output
        assert not list(tmp_path.glob("*.cdf"))


class TestVerifyCommand:
    def _write_l2(self, tmp_path):
        invoke(L2_ARGS, tmp_path)
        return tmp_path / "imap_mag_l2-calibration_20260102_v001.cdf"

    def test_a_generated_file_passes(self, tmp_path):
        path = self._write_l2(tmp_path)

        result = runner.invoke(app, ["verify", str(path)])

        assert result.exit_code == 0, result.output
        assert "All checks passed" in result.output

    def test_several_files_can_be_checked_at_once(self, tmp_path):
        path = self._write_l2(tmp_path)
        invoke(IALIRT_ARGS, tmp_path, "--zero-offsets")
        ialirt = tmp_path / "imap_mag_ialirt-calibration_20260102_v001.cdf"

        result = runner.invoke(app, ["verify", str(path), str(ialirt)])

        assert result.exit_code == 0, result.output
        assert result.output.count("All checks passed") == 2

    def test_failures_exit_non_zero(self, tmp_path):
        path = self._write_l2(tmp_path)
        with pycdf.CDF(str(path)) as cdf:
            cdf.readonly(False)
            del cdf["URFTOORFO"]

        result = runner.invoke(app, ["verify", str(path)])

        assert result.exit_code == 1
        assert "Checks failed" in result.output

    def test_a_file_that_is_not_a_calibration_file_exits_non_zero(self, tmp_path):
        path = tmp_path / "not-a-calibration-file.cdf"
        path.touch()

        result = runner.invoke(app, ["verify", str(path)])

        assert result.exit_code == 1
        assert "not a calibration filename" in result.output

    def test_missing_file_is_rejected(self, tmp_path):
        result = runner.invoke(app, ["verify", str(tmp_path / "nope.cdf")])

        assert result.exit_code != 0

    def test_warnings_are_reported_but_still_pass(self, tmp_path):
        invoke(IALIRT_ARGS, tmp_path, "--mago", "50,0,0", "--magi", "1,0,0")
        path = tmp_path / "imap_mag_ialirt-calibration_20260102_v001.cdf"

        result = runner.invoke(app, ["verify", str(path)])

        assert result.exit_code == 0, result.output
        assert "not smaller than" in result.output
        assert "All checks passed" in result.output

    def test_frame_transforms_are_shown_for_manual_review(self, tmp_path):
        path = self._write_l2(tmp_path)

        result = runner.invoke(app, ["verify", str(path)])

        assert "URFTOORFO" in result.output
        assert "URFTOORFI" in result.output


class TestHelp:
    @pytest.mark.parametrize("command", ["l2", "l1d", "ialirt", "verify"])
    def test_every_command_documents_itself(self, command):
        result = runner.invoke(app, [command, "--help"])

        assert result.exit_code == 0
        assert "Usage" in result.output

    def test_no_arguments_shows_the_available_commands(self):
        result = runner.invoke(app, [])

        for command in ("l1d", "l2", "ialirt", "verify"):
            assert command in result.output

    def test_matrix_options_are_listed(self):
        result = runner.invoke(app, ["l2", "--help"])

        assert "identity" in result.output
        assert f"v{LATEST_MATRIX_VERSION}" in result.output
