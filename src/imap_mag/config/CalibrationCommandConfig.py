from pydantic import BaseModel

from imap_mag.config.CommandConfig import CommandConfig

# Default SPICE kernel types included when generating a metakernel. Shared by the
# CalibrationApplicator and the scripted L2 calibration so both flows request the
# same set. Overridable via the ``metakernel_file_types`` app setting.
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


class SparseDatastoreConfig(BaseModel):
    """Config for building a sparse (partial) copy of the datastore in the work
    folder for the scripted L2 calibration.

    Patterns are datastore-root-relative globs, copied preserving their relative
    layout so the MATLAB script (and SPICE) resolve them from the sparse root.

    ``per_day_patterns`` are expanded for every day in
    ``[d - days_before, d + days_after]`` for each day ``d`` being calibrated.
    Supported placeholders: ``{level}`` (l1c/l1b), ``{mode}`` (norm/burst),
    ``{Y}`` (year), ``{m}`` (zero-padded month), ``{Ymd}`` (yyyymmdd). Sensor
    variants are listed explicitly.

    ``shared_patterns`` are day-independent inputs (calibration matrices,
    profiles, pre-computed offsets, spin table, thruster activities) copied once.
    They are formatted once with the run context; the ``{matrix_version}``
    placeholder keeps the (large) calibration matrices copy to just the version in
    use rather than every version on the datastore.

    The SPICE metakernel and every kernel it references are always copied
    separately (parsed from the metakernel), so they are not listed here.
    """

    days_before: int = 1
    days_after: int = 1
    per_day_patterns: list[str] = [
        # L1 science for both sensors (level/mode filled per run; day = d).
        "science/mag/{level}/{Y}/{m}/imap_mag_{level}_{mode}-mago_{Ymd}_v*.cdf",
        "science/mag/{level}/{Y}/{m}/imap_mag_{level}_{mode}-magi_{Ymd}_v*.cdf",
        # Spacecraft HK (x285 packet) used for interference/thruster cleaning.
        "hk/sc/l1/x285/{Y}/{m}/imap_sc_l1_x285_{Ymd}_v*.csv",
        # IMAP-Lo pivot-platform angle HK.
        "hk/lo/l1/pivot-platform-angle/{Y}/{m}/imap_lo_l1_pivot-platform-angle_{Ymd}_v*.csv",
    ]
    shared_patterns: list[str] = [
        # Only the calibration matrices version in use (each version is large).
        "calibration/inputs/Matrices/CalibrationMatricesV{matrix_version}.mat",
        "calibration/inputs/Matrices/CalibrationMatricesV{matrix_version}.json",
        # Interference / thruster profiles.
        "calibration/inputs/Profiles/**/*",
        # Pre-computed offsets (spin plane / spin axis / leinweber / kepko).
        "calibration/calculated_offsets/**/*",
        # Spin table + thruster activities (small, span the run).
        "spice/spin/*",
        "spice/activities/*",
    ]
    # Folders that MATLAB references with different letter-casing than they are
    # stored on the (case-insensitive) shared datastore. The MATLAB code reads
    # thruster profiles via lowercase ``inputs/profiles`` but everything else via
    # ``inputs/Profiles``; on the case-sensitive sparse copy we add a symlink for
    # the lowercase spelling so both reads resolve.
    case_insensitive_dir_aliases: list[str] = ["calibration/inputs/Profiles"]


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
