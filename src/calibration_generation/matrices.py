"""URF to ORF frame transform matrices for the MAG sensors."""

from enum import StrEnum

import numpy as np

NUM_RANGES = 4
"""Number of MAG operating ranges (0-3)."""

# Frame transform matrices, by calibration version, as (MAGo, MAGi) pairs. Each
# matrix rotates a sensor from its unit reference frame (URF) to the orthogonal
# reference frame (ORF). The same matrix is used for every operating range.
MATRICES_BY_VERSION: dict[int, tuple[list[list[float]], list[list[float]]]] = {
    1: (
        [
            [0.999989918615689, -3.25282523386058e-05, -0.00477484817389527],
            [-0.00161934234084477, 1.00081282729992, -0.0107947265795152],
            [0.00449876643714237, 0.0068123135754105, 0.999984957597784],
        ],
        [
            [1.0011485845956, -0.00335794534947757, -0.00337416446398934],
            [0.000583975986099036, 1.00042889375192, -0.00962198497343362],
            [0.0050384296101901, 0.00443047384468987, 0.999976121125062],
        ],
    ),
    8: (
        [
            [0.999987602534581, -4.99214226808116e-05, -0.00581853312312828],
            [-0.00108681739014099, 1.0008646579327, -0.00876782778104443],
            [0.00503997052208487, 0.00857925414956133, 0.999944948687242],
        ],
        [
            [1.0011485845956, -0.00335794534947757, -0.00337416446398934],
            [0.000583975986099036, 1.00042889375192, -0.00962198497343362],
            [0.0050384296101901, 0.00443047384468987, 0.999976121125062],
        ],
    ),
    9: (
        [
            [0.9999873709294, -4.64936676946261e-05, -0.00541906518022556],
            [-0.00108115033777228, 1.0008650589839, -0.00764394952398624],
            [0.00504001793113667, 0.00857927489534269, 0.99995659973086],
        ],
        [
            [1.00135071901479, -0.00349687971141703, -0.00536642675389405],
            [0.00114590105448703, 1.00060703082339, -0.0057125194325257],
            [0.00514475513863999, 0.00459663720158882, 0.999969917168983],
        ],
    ),
}

LATEST_MATRIX_VERSION = max(MATRICES_BY_VERSION)


class MatrixSet(StrEnum):
    """Which set of frame transform matrices to write to a calibration file."""

    IDENTITY = "identity"
    LATEST = "latest"
    V1 = "v1"
    V8 = "v8"
    V9 = "v9"

    @property
    def version(self) -> int | None:
        """Calibration version this set refers to, or None for the identity set."""
        if self is MatrixSet.IDENTITY:
            return None
        if self is MatrixSet.LATEST:
            return LATEST_MATRIX_VERSION
        return int(self.value.removeprefix("v"))


def stack_over_ranges(matrix: list[list[float]] | np.ndarray) -> np.ndarray:
    """
    Repeat a single 3x3 matrix once per operating range.

    Args:
        matrix: A 3x3 frame transform matrix.

    Returns:
        Array of shape (3, 3, NUM_RANGES), as stored in the CDF.
    """
    matrix = np.asarray(matrix, dtype=float)
    if matrix.shape != (3, 3):
        raise ValueError(f"Frame transform must be 3x3, got {matrix.shape}")
    return np.stack([matrix] * NUM_RANGES, axis=2)


def get_frame_transforms(matrix_set: MatrixSet) -> tuple[np.ndarray, np.ndarray]:
    """
    Build the per-range MAGo and MAGi frame transforms for a matrix set.

    Args:
        matrix_set: Which set of matrices to use.

    Returns:
        The (MAGo, MAGi) frame transforms, each of shape (3, 3, NUM_RANGES).
    """
    version = matrix_set.version

    if version is None:
        identity = np.eye(3)
        return stack_over_ranges(identity), stack_over_ranges(identity)

    mago, magi = MATRICES_BY_VERSION[version]
    return stack_over_ranges(mago), stack_over_ranges(magi)


def describe(matrix_set: MatrixSet) -> str:
    """Return a short human readable description of a matrix set."""
    version = matrix_set.version

    if version is None:
        return "identity matrices (no rotation)"

    return f"calibration version {version} matrices"
