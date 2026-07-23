from pathlib import Path

from imap_mag.data_pipelines import PROGRESS_DATE_CONTEXT_KEY, FileRecord, Record, Stage
from imap_mag.download.FetchIALiRT import FetchIALiRT
from imap_mag.io.file.IALiRTPathHandler import IALiRTPathHandler
from imap_mag.util.DatetimeProvider import DatetimeProvider


class DownloadIALiRTStage(Stage):
    """Download I-ALiRT data for a specific instrument and emit one FileRecord per day."""

    def __init__(
        self,
        instrument: str,
        fetcher: FetchIALiRT,
        datetime_provider: DatetimeProvider = DatetimeProvider(),
    ):
        super().__init__()
        self.instrument = instrument
        self.fetcher = fetcher
        self._datetime_provider = datetime_provider

    async def process(self, item: Record, context: dict, **kwargs):

        start_date = getattr(item, "start_date", None)
        end_date = getattr(item, "end_date", None)

        if not item or not start_date or not end_date:
            raise ValueError(
                "DownloadIALiRTStage requires a Record with start_date and end_date"
            )

        self.logger.info(
            f"Downloading I-ALiRT {self.instrument} data from {start_date} to {end_date}."
        )

        if self.instrument.endswith("_hk"):
            downloaded: dict[Path, IALiRTPathHandler] = (
                self.fetcher.download_instrument_data(
                    instrument=self.instrument,
                    start_date=start_date,
                    end_date=end_date,
                    housekeeping=True,
                )
            )  # type: ignore
        else:
            downloaded: dict[Path, IALiRTPathHandler] = (
                self.fetcher.download_instrument_data(
                    instrument=self.instrument,
                    start_date=start_date,
                    end_date=end_date,
                )
            )  # type: ignore

        if not downloaded:
            self.logger.info(
                f"No I-ALiRT {self.instrument} data downloaded from {start_date} to {end_date}."
            )
            return

        # update progress
        for file_path, path_handler in downloaded.items():
            context[PROGRESS_DATE_CONTEXT_KEY] = end_date

            # Stream file to the next stage
            await self.publish_next(
                FileRecord(file_path, end_date),  # type: ignore
                context,
                **kwargs,  # type: ignore
            )
