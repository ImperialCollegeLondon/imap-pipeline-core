"""Tests for the calibration_generation.matrices module."""

import numpy as np
import pytest

from calibration_generation.matrices import (
    LATEST_MATRIX_VERSION,
    MATRICES_BY_VERSION,
    NUM_RANGES,
    MatrixSet,
    describe,
    get_frame_transforms,
    stack_over_ranges,
)


class TestStackOverRanges:
    def test_repeats_matrix_once_per_range(self):
        stacked = stack_over_ranges(np.eye(3))

        assert stacked.shape == (3, 3, NUM_RANGES)
        for range_index in range(NUM_RANGES):
            np.testing.assert_array_equal(stacked[:, :, range_index], np.eye(3))

    def test_accepts_nested_lists(self):
        stacked = stack_over_ranges([[1, 2, 3], [4, 5, 6], [7, 8, 9]])

        assert stacked.shape == (3, 3, NUM_RANGES)
        assert stacked[0, 2, 0] == 3

    def test_rejects_matrix_that_is_not_3x3(self):
        with pytest.raises(ValueError, match="must be 3x3"):
            stack_over_ranges(np.eye(2))


class TestMatrixSet:
    def test_identity_has_no_version(self):
        assert MatrixSet.IDENTITY.version is None

    def test_latest_resolves_to_highest_defined_version(self):
        assert MatrixSet.LATEST.version == LATEST_MATRIX_VERSION
        assert MatrixSet.LATEST.version == max(MATRICES_BY_VERSION)

    @pytest.mark.parametrize(
        ("matrix_set", "expected"),
        [(MatrixSet.V1, 1), (MatrixSet.V8, 8), (MatrixSet.V9, 9)],
    )
    def test_versioned_sets_resolve_to_their_version(self, matrix_set, expected):
        assert matrix_set.version == expected

    def test_every_versioned_set_has_matrices_defined(self):
        for matrix_set in MatrixSet:
            if matrix_set.version is not None:
                assert matrix_set.version in MATRICES_BY_VERSION


class TestGetFrameTransforms:
    def test_identity_returns_identity_for_both_sensors(self):
        mago, magi = get_frame_transforms(MatrixSet.IDENTITY)

        for range_index in range(NUM_RANGES):
            np.testing.assert_array_equal(mago[:, :, range_index], np.eye(3))
            np.testing.assert_array_equal(magi[:, :, range_index], np.eye(3))

    def test_latest_matches_the_highest_version(self):
        np.testing.assert_array_equal(
            get_frame_transforms(MatrixSet.LATEST)[0],
            get_frame_transforms(MatrixSet(f"v{LATEST_MATRIX_VERSION}"))[0],
        )

    @pytest.mark.parametrize("matrix_set", list(MatrixSet))
    def test_all_sets_are_near_identity_rotations(self, matrix_set):
        """Frame transforms correct for small misalignments, so stay close to I."""
        for transforms in get_frame_transforms(matrix_set):
            assert transforms.shape == (3, 3, NUM_RANGES)
            for range_index in range(NUM_RANGES):
                matrix = transforms[:, :, range_index]
                assert np.linalg.det(matrix) == pytest.approx(1.0, abs=0.01)
                np.testing.assert_allclose(matrix, np.eye(3), atol=0.02)

    def test_sensors_differ_for_real_matrices(self):
        mago, magi = get_frame_transforms(MatrixSet.V9)

        assert not np.array_equal(mago, magi)


class TestDescribe:
    def test_identity_is_described_without_a_version(self):
        assert describe(MatrixSet.IDENTITY) == "identity matrices (no rotation)"

    def test_version_is_named(self):
        assert describe(MatrixSet.V8) == "calibration version 8 matrices"
