from pydantic import BaseModel

from imap_mag.config.CommandConfig import CommandConfig

# Fallback for the ``metakernel_file_types`` app setting when it is not provided by
# the yaml config. The authoritative list lives in the AppSettings yaml file and is
# shared by the CalibrationApplicator and the scripted L2 calibration.
DEFAULT_METAKERNEL_FILE_TYPES: list[str] = [
    "leapseconds",
    "planetary_constants",
    "science_frames",
    "imap_frames",
    "spacecraft_clock",
    "attitude_history",
    "pointing_attitude",
    "planetary_ephemeris",
    "ephemeris_reconstructed",
]


class SparseDatastorePattern(BaseModel):
    """A single datastore-root-relative glob used to build the sparse datastore.

    ``pattern`` is copied preserving its relative layout. Dates use Python
    ``strftime`` codes (e.g. ``%Y`` for the 4-digit year, ``%y`` for 2-digit,
    ``%m`` month, ``%d`` day); ``{level}`` (l1c/l1b), ``{mode}`` (norm/burst) and
    ``{matrix_version}`` are filled from the run.

    ``days_before``/``days_after`` widen the copy to neighbouring days around each
    day being calibrated. They default to 0 (only the day itself) so large per-day
    files such as burst science are not copied for days that are not needed.
    """

    pattern: str
    days_before: int = 0
    days_after: int = 0


class SparseDatastoreConfig(BaseModel):
    """Patterns for building a sparse datastore copy in the work folder.

    The authoritative pattern list lives in the AppSettings yaml file under
    ``calibrate.sparse_datastore.patterns``; the SPICE metakernel and the kernels
    it references are always copied separately (parsed from the metakernel).
    """

    patterns: list[SparseDatastorePattern] = []


class CalibrationCommandConfig(CommandConfig):
    """Command configuration for the calibrate flow.

    ``work_sub_folder`` supports ``{date}``/``{mode}``/``{sensor}`` placeholders so
    each calibrate run gets its own uniquely-named work folder.
    """

    work_sub_folder: str | None = "calibrate_{date}_{mode}"

    # Scripted L2 MATLAB timeouts, per day of data processed, by mode.
    scripted_l2_timeout_seconds_per_day_norm: int = 10 * 60
    scripted_l2_timeout_seconds_per_day_burst: int = 60 * 60

    sparse_datastore: SparseDatastoreConfig = SparseDatastoreConfig()
