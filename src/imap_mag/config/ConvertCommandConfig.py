from imap_mag.config.CommandConfig import CommandConfig


class ConvertCommandConfig(CommandConfig):
    """Command configuration for the calibrate-convert flow."""

    work_sub_folder: str | None = "convert"
    publish_to_data_store: bool = True
