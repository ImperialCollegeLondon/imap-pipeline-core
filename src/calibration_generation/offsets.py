"""Entry, validation and display of MAG sensor offsets."""

import sys
from pathlib import Path

import numpy as np
import yaml
from rich.prompt import Prompt
from rich.table import Table

from calibration_generation.matrices import NUM_RANGES

SENSORS: tuple[str, str] = ("MAGo", "MAGi")
"""Sensor order used by the sensor axis of the offsets array."""

NUM_AXES = 3
AXIS_LABELS = ("X", "Y", "Z")

OFFSETS_SHAPE = (len(SENSORS), NUM_RANGES, NUM_AXES)
"""Shape of the offsets array stored in the CDF: (sensor, range, axis)."""

EXAMPLE_FILE_CONTENT = """\
# One vector per sensor, applied to every range:
MAGo: [-11.2, 0.4, 3.1]
MAGi: [-21.9, 1.0, 4.2]

# ...or one vector per range, in range order 0, 1, 2, 3:
# MAGo:
#   - [-11.2, 0.4, 3.1]
#   - [-11.3, 0.4, 3.1]
#   - [-11.4, 0.5, 3.2]
#   - [-11.6, 0.5, 3.2]
"""


class OffsetError(ValueError):
    """Raised when offsets cannot be parsed or are not the expected shape."""


def zero_offsets() -> np.ndarray:
    """Return an all-zero offsets array of shape (sensor, range, axis)."""
    return np.zeros(OFFSETS_SHAPE)


def parse_offset_vector(text: str) -> np.ndarray:
    """
    Parse a single offset vector.

    Accepts comma or whitespace separated values, either as one vector
    ("1,2,3") applied to every range, or as one vector per range
    ("1,2,3; 1,2,3; 1,2,3; 1,2,3").

    Args:
        text: The vector(s) to parse.

    Returns:
        Array of shape (NUM_RANGES, NUM_AXES).

    Raises:
        OffsetError: If the text is not a valid set of offset vectors.
    """
    groups = [group for group in text.split(";") if group.strip()]

    if len(groups) not in (1, NUM_RANGES):
        raise OffsetError(
            f"Expected 1 or {NUM_RANGES} semicolon-separated vectors, got {len(groups)}"
        )

    vectors = []
    for group in groups:
        values = [value for value in group.replace(",", " ").split() if value]
        if len(values) != NUM_AXES:
            raise OffsetError(
                f"Expected {NUM_AXES} values (X, Y, Z) in {group.strip()!r}, "
                f"got {len(values)}"
            )
        try:
            vectors.append([float(value) for value in values])
        except ValueError as error:
            raise OffsetError(f"{group.strip()!r} is not a set of numbers") from error

    if len(vectors) == 1:
        vectors = vectors * NUM_RANGES

    return np.array(vectors, dtype=float)


def build_offsets(mago: np.ndarray, magi: np.ndarray) -> np.ndarray:
    """
    Combine per-sensor offsets into the array layout used by the CDF.

    Args:
        mago: MAGo offsets, of shape (NUM_RANGES, NUM_AXES).
        magi: MAGi offsets, of shape (NUM_RANGES, NUM_AXES).

    Returns:
        Array of shape (sensor, range, axis).

    Raises:
        OffsetError: If either sensor's offsets are the wrong shape.
    """
    expected = (NUM_RANGES, NUM_AXES)

    for sensor, offsets in zip(SENSORS, (mago, magi)):
        if offsets.shape != expected:
            raise OffsetError(
                f"{sensor} offsets must have shape {expected}, got {offsets.shape}"
            )

    return np.stack([mago, magi], axis=0)


def load_offsets_file(path: Path) -> np.ndarray:
    """
    Load offsets from a YAML or JSON file keyed by sensor name.

    Args:
        path: File containing a MAGo and a MAGi entry, each holding either one
            [X, Y, Z] vector or one vector per range.

    Returns:
        Array of shape (sensor, range, axis).

    Raises:
        OffsetError: If the file is malformed or a sensor entry is missing.
    """
    try:
        content = yaml.safe_load(path.read_text())
    except yaml.YAMLError as error:
        raise OffsetError(f"{path} is not valid YAML or JSON: {error}") from error

    if not isinstance(content, dict):
        raise OffsetError(
            f"{path} must contain a mapping of sensor name to offsets, "
            f"for example:\n{EXAMPLE_FILE_CONTENT}"
        )

    by_sensor = {str(key).casefold(): value for key, value in content.items()}
    parsed = []

    for sensor in SENSORS:
        if sensor.casefold() not in by_sensor:
            raise OffsetError(f"{path} has no offsets for {sensor}")

        vectors = np.asarray(by_sensor[sensor.casefold()], dtype=float)
        if vectors.shape == (NUM_AXES,):
            vectors = np.stack([vectors] * NUM_RANGES, axis=0)
        parsed.append(vectors)

    return build_offsets(*parsed)


def prompt_for_offsets() -> np.ndarray:
    """
    Ask for one offset vector per sensor, re-prompting until each is valid.

    Returns:
        Array of shape (sensor, range, axis).

    Raises:
        OffsetError: If there is no terminal to prompt on.
    """
    if not sys.stdin.isatty():
        raise OffsetError(
            "No offsets given and no terminal to prompt on. Pass --mago and "
            "--magi, or --offsets-file, or --zero-offsets."
        )

    print(
        "Enter offsets in nT as 'X,Y,Z' to use for every range, or four "
        "semicolon-separated vectors to set each range individually."
    )

    vectors = []
    for sensor in SENSORS:
        while True:
            try:
                vectors.append(parse_offset_vector(Prompt.ask(f"{sensor} offsets")))
                break
            except OffsetError as error:
                print(f"  {error}. Please try again.")

    return build_offsets(*vectors)


def resolve_offsets(
    mago: str | None,
    magi: str | None,
    offsets_file: Path | None,
    use_zeros: bool,
) -> np.ndarray:
    """
    Work out the offsets to write from the options the user supplied.

    Args:
        mago: MAGo offset vector(s) as text, or None if not supplied.
        magi: MAGi offset vector(s) as text, or None if not supplied.
        offsets_file: Path to an offsets file, or None if not supplied.
        use_zeros: Whether zero offsets were explicitly requested.

    Returns:
        Array of shape (sensor, range, axis).

    Raises:
        OffsetError: If the options conflict, or offsets cannot be determined.
    """
    inline = mago is not None or magi is not None

    if offsets_file is not None and inline:
        raise OffsetError("Use either --offsets-file or --mago/--magi, not both")
    if use_zeros and (inline or offsets_file is not None):
        raise OffsetError("--zero-offsets cannot be combined with other offset options")

    if use_zeros:
        return zero_offsets()

    if offsets_file is not None:
        return load_offsets_file(offsets_file)

    if inline:
        zeros = np.zeros((NUM_RANGES, NUM_AXES))
        return build_offsets(
            parse_offset_vector(mago) if mago else zeros,
            parse_offset_vector(magi) if magi else zeros,
        )

    return prompt_for_offsets()


def magnitude_warnings(offsets: np.ndarray) -> list[str]:
    """
    Check offsets against expectations about the two sensors.

    MAGi sits closer to the spacecraft than MAGo, so it sees more of the
    spacecraft field and its offsets are expected to be larger. Ranges where
    neither sensor has an offset are skipped, since zero offsets are a
    deliberate choice rather than a suspicious pair of magnitudes.

    Args:
        offsets: Offsets of shape (sensor, range, axis).

    Returns:
        One message per range that does not meet expectations, empty if all do.
    """
    warnings = []

    for range_index in range(NUM_RANGES):
        mago_magnitude = float(np.linalg.norm(offsets[0, range_index, :]))
        magi_magnitude = float(np.linalg.norm(offsets[1, range_index, :]))

        if mago_magnitude == 0 and magi_magnitude == 0:
            continue

        if mago_magnitude >= magi_magnitude:
            warnings.append(
                f"Range {range_index}: |MAGo| ({mago_magnitude:.3f} nT) is not "
                f"smaller than |MAGi| ({magi_magnitude:.3f} nT)"
            )

    return warnings


def offsets_table(offsets: np.ndarray) -> Table:
    """
    Render offsets for review before writing them to a file.

    Ranges sharing the same offsets are collapsed into a single row.

    Args:
        offsets: Offsets of shape (sensor, range, axis).

    Returns:
        A table of offsets and their magnitudes.
    """
    table = Table(title="Offsets (nT)")
    table.add_column("Sensor")
    table.add_column("Range")
    for label in AXIS_LABELS:
        table.add_column(label, justify="right")
    table.add_column("Magnitude", justify="right")

    for sensor_index, sensor in enumerate(SENSORS):
        sensor_offsets = offsets[sensor_index]
        if np.all(sensor_offsets == sensor_offsets[0]):
            rows = [(f"0-{NUM_RANGES - 1}", sensor_offsets[0])]
        else:
            rows = [(str(index), vector) for index, vector in enumerate(sensor_offsets)]

        for range_label, vector in rows:
            table.add_row(
                sensor,
                range_label,
                *(f"{value:.4g}" for value in vector),
                f"{np.linalg.norm(vector):.4g}",
            )

    return table
