"""Tests for the calibration_generation.offsets module."""

import json
from unittest.mock import patch

import numpy as np
import pytest

from calibration_generation.matrices import NUM_RANGES
from calibration_generation.offsets import (
    OFFSETS_SHAPE,
    OffsetError,
    build_offsets,
    load_offsets_file,
    magnitude_warnings,
    offsets_table,
    parse_offset_vector,
    prompt_for_offsets,
    resolve_offsets,
    zero_offsets,
)


class TestZeroOffsets:
    def test_has_the_cdf_shape_and_is_all_zero(self):
        offsets = zero_offsets()

        assert offsets.shape == OFFSETS_SHAPE
        assert not offsets.any()


class TestParseOffsetVector:
    @pytest.mark.parametrize("text", ["1,2,3", "1, 2, 3", "1 2 3", " 1,2,3 ", "1,2,3;"])
    def test_single_vector_is_applied_to_every_range(self, text):
        parsed = parse_offset_vector(text)

        assert parsed.shape == (NUM_RANGES, 3)
        for range_index in range(NUM_RANGES):
            np.testing.assert_array_equal(parsed[range_index], [1.0, 2.0, 3.0])

    def test_one_vector_per_range(self):
        parsed = parse_offset_vector("1,1,1; 2,2,2; 3,3,3; 4,4,4")

        assert parsed.shape == (NUM_RANGES, 3)
        np.testing.assert_array_equal(parsed[2], [3.0, 3.0, 3.0])

    def test_negative_and_scientific_values(self):
        parsed = parse_offset_vector("-11.2,4e-2,+3.1")

        np.testing.assert_allclose(parsed[0], [-11.2, 0.04, 3.1])

    @pytest.mark.parametrize(
        ("text", "message"),
        [
            ("1,2", "Expected 3 values"),
            ("1,2,3,4", "Expected 3 values"),
            ("1,2,3; 4,5,6", "semicolon-separated"),
            ("a,b,c", "not a set of numbers"),
            ("", "semicolon-separated"),
        ],
    )
    def test_invalid_input_is_rejected(self, text, message):
        with pytest.raises(OffsetError, match=message):
            parse_offset_vector(text)


class TestBuildOffsets:
    def test_sensor_order_is_mago_then_magi(self):
        mago = np.ones((NUM_RANGES, 3))
        magi = np.full((NUM_RANGES, 3), 2.0)

        offsets = build_offsets(mago, magi)

        assert offsets.shape == OFFSETS_SHAPE
        np.testing.assert_array_equal(offsets[0], mago)
        np.testing.assert_array_equal(offsets[1], magi)

    def test_wrong_shape_is_rejected_and_names_the_sensor(self):
        with pytest.raises(OffsetError, match="MAGi offsets must have shape"):
            build_offsets(np.zeros((NUM_RANGES, 3)), np.zeros((3,)))


class TestLoadOffsetsFile:
    def test_loads_one_vector_per_sensor_from_yaml(self, tmp_path):
        path = tmp_path / "offsets.yaml"
        path.write_text("MAGo: [-11.2, 0.4, 3.1]\nMAGi: [-21.9, 1.0, 4.2]\n")

        offsets = load_offsets_file(path)

        assert offsets.shape == OFFSETS_SHAPE
        for range_index in range(NUM_RANGES):
            np.testing.assert_allclose(offsets[0, range_index], [-11.2, 0.4, 3.1])
            np.testing.assert_allclose(offsets[1, range_index], [-21.9, 1.0, 4.2])

    def test_loads_json(self, tmp_path):
        path = tmp_path / "offsets.json"
        path.write_text(json.dumps({"MAGo": [1, 2, 3], "MAGi": [4, 5, 6]}))

        offsets = load_offsets_file(path)

        np.testing.assert_array_equal(offsets[1, 0], [4.0, 5.0, 6.0])

    def test_loads_one_vector_per_range(self, tmp_path):
        path = tmp_path / "offsets.yaml"
        path.write_text(
            "MAGo:\n"
            "  - [1, 1, 1]\n"
            "  - [2, 2, 2]\n"
            "  - [3, 3, 3]\n"
            "  - [4, 4, 4]\n"
            "MAGi: [9, 9, 9]\n"
        )

        offsets = load_offsets_file(path)

        np.testing.assert_array_equal(offsets[0, 3], [4.0, 4.0, 4.0])
        np.testing.assert_array_equal(offsets[1, 3], [9.0, 9.0, 9.0])

    def test_sensor_names_are_case_insensitive(self, tmp_path):
        path = tmp_path / "offsets.yaml"
        path.write_text("mago: [1, 2, 3]\nMAGI: [4, 5, 6]\n")

        offsets = load_offsets_file(path)

        np.testing.assert_array_equal(offsets[0, 0], [1.0, 2.0, 3.0])

    def test_missing_sensor_is_reported(self, tmp_path):
        path = tmp_path / "offsets.yaml"
        path.write_text("MAGo: [1, 2, 3]\n")

        with pytest.raises(OffsetError, match="no offsets for MAGi"):
            load_offsets_file(path)

    def test_wrong_number_of_ranges_is_reported(self, tmp_path):
        path = tmp_path / "offsets.yaml"
        path.write_text("MAGo:\n  - [1, 1, 1]\n  - [2, 2, 2]\nMAGi: [9, 9, 9]\n")

        with pytest.raises(OffsetError, match="MAGo offsets must have shape"):
            load_offsets_file(path)

    def test_non_mapping_content_shows_an_example(self, tmp_path):
        path = tmp_path / "offsets.yaml"
        path.write_text("- 1\n- 2\n")

        with pytest.raises(OffsetError, match="mapping of sensor name"):
            load_offsets_file(path)

    def test_invalid_yaml_is_reported(self, tmp_path):
        path = tmp_path / "offsets.yaml"
        path.write_text("MAGo: [1, 2,\nMAGi: ]]\n")

        with pytest.raises(OffsetError, match="not valid YAML or JSON"):
            load_offsets_file(path)


class TestResolveOffsets:
    def test_zero_flag_gives_zeros(self):
        offsets = resolve_offsets(None, None, None, use_zeros=True)

        assert not offsets.any()

    def test_inline_vectors_are_parsed(self):
        offsets = resolve_offsets("1,2,3", "4,5,6", None, use_zeros=False)

        np.testing.assert_array_equal(offsets[0, 0], [1.0, 2.0, 3.0])
        np.testing.assert_array_equal(offsets[1, 0], [4.0, 5.0, 6.0])

    def test_omitted_sensor_defaults_to_zero(self):
        offsets = resolve_offsets(None, "4,5,6", None, use_zeros=False)

        assert not offsets[0].any()
        np.testing.assert_array_equal(offsets[1, 0], [4.0, 5.0, 6.0])

    def test_file_is_used_when_given(self, tmp_path):
        path = tmp_path / "offsets.yaml"
        path.write_text("MAGo: [1, 2, 3]\nMAGi: [4, 5, 6]\n")

        offsets = resolve_offsets(None, None, path, use_zeros=False)

        np.testing.assert_array_equal(offsets[0, 0], [1.0, 2.0, 3.0])

    def test_file_and_inline_together_are_rejected(self, tmp_path):
        with pytest.raises(OffsetError, match="not both"):
            resolve_offsets("1,2,3", None, tmp_path / "offsets.yaml", use_zeros=False)

    def test_zero_flag_with_other_options_is_rejected(self):
        with pytest.raises(OffsetError, match="cannot be combined"):
            resolve_offsets("1,2,3", None, None, use_zeros=True)

    def test_prompts_when_nothing_is_given(self):
        with patch("calibration_generation.offsets.prompt_for_offsets") as mock_prompt:
            mock_prompt.return_value = zero_offsets()

            resolve_offsets(None, None, None, use_zeros=False)

        mock_prompt.assert_called_once()


class TestPromptForOffsets:
    def test_asks_once_per_sensor(self):
        with (
            patch("sys.stdin.isatty", return_value=True),
            patch(
                "calibration_generation.offsets.Prompt.ask",
                side_effect=["1,2,3", "4,5,6"],
            ) as mock_ask,
        ):
            offsets = prompt_for_offsets()

        assert mock_ask.call_count == 2
        np.testing.assert_array_equal(offsets[0, 0], [1.0, 2.0, 3.0])
        np.testing.assert_array_equal(offsets[1, 0], [4.0, 5.0, 6.0])

    def test_reprompts_after_invalid_input(self):
        with (
            patch("sys.stdin.isatty", return_value=True),
            patch(
                "calibration_generation.offsets.Prompt.ask",
                side_effect=["nonsense", "1,2,3", "4,5,6"],
            ) as mock_ask,
        ):
            offsets = prompt_for_offsets()

        assert mock_ask.call_count == 3
        np.testing.assert_array_equal(offsets[0, 0], [1.0, 2.0, 3.0])

    def test_fails_helpfully_without_a_terminal(self):
        with patch("sys.stdin.isatty", return_value=False):
            with pytest.raises(OffsetError, match="no terminal to prompt on"):
                prompt_for_offsets()


class TestMagnitudeWarnings:
    def test_no_warnings_when_magi_offsets_are_larger(self):
        offsets = build_offsets(np.ones((NUM_RANGES, 3)), np.full((NUM_RANGES, 3), 2.0))

        assert magnitude_warnings(offsets) == []

    def test_all_zero_offsets_are_not_warned_about(self):
        assert magnitude_warnings(zero_offsets()) == []

    def test_warns_when_mago_offsets_are_larger(self):
        offsets = build_offsets(np.full((NUM_RANGES, 3), 5.0), np.ones((NUM_RANGES, 3)))

        warnings = magnitude_warnings(offsets)

        assert len(warnings) == NUM_RANGES
        assert "Range 0" in warnings[0]

    def test_warns_only_for_the_offending_range(self):
        offsets = build_offsets(np.ones((NUM_RANGES, 3)), np.full((NUM_RANGES, 3), 2.0))
        offsets[1, 2, :] = 0.1

        warnings = magnitude_warnings(offsets)

        assert len(warnings) == 1
        assert "Range 2" in warnings[0]

    def test_equal_non_zero_magnitudes_are_warned_about(self):
        offsets = build_offsets(np.ones((NUM_RANGES, 3)), np.ones((NUM_RANGES, 3)))

        assert len(magnitude_warnings(offsets)) == NUM_RANGES


class TestOffsetsTable:
    def _rendered(self, table) -> str:
        from rich.console import Console

        console = Console(width=120, record=True)
        console.print(table)
        return console.export_text()

    def test_identical_ranges_are_collapsed_into_one_row(self):
        offsets = build_offsets(np.ones((NUM_RANGES, 3)), np.full((NUM_RANGES, 3), 2.0))

        rendered = self._rendered(offsets_table(offsets))

        assert f"0-{NUM_RANGES - 1}" in rendered
        assert rendered.count("MAGo") == 1

    def test_differing_ranges_are_listed_separately(self):
        offsets = zero_offsets()
        offsets[0, 1, :] = [1.0, 2.0, 3.0]

        rendered = self._rendered(offsets_table(offsets))

        assert rendered.count("MAGo") == NUM_RANGES

    def test_magnitude_is_shown(self):
        offsets = zero_offsets()
        offsets[0, :, :] = [3.0, 4.0, 0.0]

        rendered = self._rendered(offsets_table(offsets))

        assert "5" in rendered
