from imap_mag.config.ApiSource import IALiRTApiSource, SdcApiSource, WebPodaApiSource
from imap_mag.config.CommandConfig import CommandConfig


class FetchBinaryConfig(CommandConfig):
    api: WebPodaApiSource
    publish_to_data_store: bool = True


class FetchIALiRTConfig(CommandConfig):
    api: IALiRTApiSource
    publish_to_data_store: bool = True


class FetchScienceConfig(CommandConfig):
    api: SdcApiSource
    publish_to_data_store: bool = True


class FetchSpiceConfig(CommandConfig):
    api: SdcApiSource
    publish_to_data_store: bool = True
