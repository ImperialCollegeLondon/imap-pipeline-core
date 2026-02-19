from enum import StrEnum


class FetchMode(StrEnum):
    DownloadOnly = "DownloadOnly"
    DownloadAndUpdateProgress = "DownloadAndUpdateProgress"
