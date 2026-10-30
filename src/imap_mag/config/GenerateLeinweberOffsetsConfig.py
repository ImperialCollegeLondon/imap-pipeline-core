from imap_mag.config.CommandConfig import CommandConfig


class GenerateLeinweberOffsetsConfig(CommandConfig):
    """Command configuration for the generate-leinweber-offsets flow.

    ``work_sub_folder`` supports ``{date}``/``{duration}`` placeholders so each run
    gets its own uniquely named work folder.
    """

    work_sub_folder: str | None = "leinweber_{date}_{duration}hr"

    # Default calibration matrix version and solar-wind-interval config used by
    # generate_raw_leinweber_offset when not overridden on the command line.
    calibration_matrix_version: int = 9
    calibration_input_json_file: str = (
        "+calibration/calibration/input_files/template.json"
    )
    duration_hours: int = 24
    timeout_seconds_per_day: int = 10 * 60
