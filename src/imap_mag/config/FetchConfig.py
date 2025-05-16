from imap_mag.config.ApiSource import ApiSource
from imap_mag.config.CommandConfig import CommandConfig


class FetchConfig(CommandConfig):
    api: ApiSource
    publish_to_data_store: bool = True
