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

    timeout_seconds_per_day: int = 10 * 60
