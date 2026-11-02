from imap_mag.config.CommandConfig import CommandConfig


class GenerateLeinweberOffsetsConfig(CommandConfig):
    """Command configuration for the generate-leinweber-offsets flow.

    Nested under ``CalibrationCommandConfig`` (``calibrate.leinweber`` in
    imap-mag-config.yaml) since it is one of the calibrate flow's MATLAB tools -
    ``calibration_matrix_version``/``input_json_file`` are shared with the
    scripted-L2 calibration and live on the parent config instead of being
    duplicated here.

    ``work_sub_folder`` supports ``{date}``/``{duration}`` placeholders so each run
    gets its own uniquely named work folder.
    """

    work_sub_folder: str | None = "leinweber_{date}_{duration}hr"

    duration_hours: int = 24
    timeout_seconds_per_day: int = 10 * 60

    # When the flow is run with no explicit start_date (e.g. the daily schedule),
    # it sweeps the last N days ending yesterday rather than just processing
    # yesterday - L1C norm science typically lands 3-4 days after the day it
    # covers, so a single-day default would almost always find nothing ready.
    # Days that are already published or still not ready are cheap no-ops, so a
    # generous window costs little. Overridable per-run (only used when the flow
    # is given no start_date) via the flow's own backfill_lookback_days parameter.
    backfill_lookback_days: int = 14
