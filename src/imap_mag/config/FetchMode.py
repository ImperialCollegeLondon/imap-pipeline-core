from enum import Enum


class FetchMode(str, Enum):
    DownloadOnly = "DownloadOnly"
    DownloadAndUpdateProgress = "DownloadAndUpdateProgress"
