from pathlib import Path

from imap_mag.data_pipelines import PROGRESS_DATE_CONTEXT_KEY, FileRecord, Record, Stage
from imap_mag.download.FetchIALiRT import FetchIALiRT
from imap_mag.io.file.IFilePathHandler import IFilePathHandler


class DownloadIALiRTStage(Stage):
    """Download I-ALiRT data for a specific instrument and emit one FileRecord per day."""

    def __init__(self, instrument: str, fetcher: FetchIALiRT):
        super().__init__()
        self.instrument = instrument
        self.fetcher = fetcher

    async def process(self, item: Record, context: dict, **kwargs):
        if not item or not item.start_date or not item.end_date:
            raise ValueError(
                "DownloadIALiRTStage requires a Record with start_date and end_date"
            )

        start_date = item.start_date
        end_date = item.end_date

        self.logger.info(
            f"Downloading I-ALiRT {self.instrument} data from {start_date} to {end_date}."
        )

        downloaded: dict[Path, IFilePathHandler] = (
            self.fetcher.download_instrument_to_csv(
                instrument=self.instrument,
                start_date=start_date,
                end_date=end_date,
            )
        )

        if not downloaded:
            self.logger.info(
                f"No I-ALiRT {self.instrument} data downloaded from {start_date} to {end_date}."
            )
            return

        for file_path, path_handler in downloaded.items():
            content_date = path_handler.get_content_date_for_indexing()

            if content_date and (
                context.get(PROGRESS_DATE_CONTEXT_KEY) is None
                or content_date > context[PROGRESS_DATE_CONTEXT_KEY]
            ):
                context[PROGRESS_DATE_CONTEXT_KEY] = content_date

            await self.publish_next(
                FileRecord(file_path, content_date), context, **kwargs
            )
