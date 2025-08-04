from imap_mag.config.ApiSource import SdcApiSource, WebPodaApiSource
from imap_mag.config.CommandConfig import CommandConfig


class FetchBinaryConfig(CommandConfig):
    api: WebPodaApiSource
    publish_to_data_store: bool = True


class FetchScienceConfig(CommandConfig):
    api: SdcApiSource
    publish_to_data_store: bool = True
