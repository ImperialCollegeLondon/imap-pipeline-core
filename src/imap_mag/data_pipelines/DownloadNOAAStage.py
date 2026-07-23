from imap_mag.data_pipelines import PROGRESS_DATE_CONTEXT_KEY, FileRecord, Record, Stage
from imap_mag.download.FetchNOAA import FetchNOAA


class DownloadNOAAStage(Stage):
    """Download NOAA RTSW data for a spacecraft/instrument and emit one FileRecord per downloaded day."""

    def __init__(
        self,
        spacecraft: str,
        instrument: str,
        fetcher: FetchNOAA,
    ):
        """Initialise the stage.

        Args:
            spacecraft: Spacecraft to download data for. Must be 'SOLAR1' or 'ACE'.
            instrument: Instrument to download data for. Must be 'mag' or 'plasma'.
            fetcher: FetchNOAA instance used to perform the download.
        """
        super().__init__()
        self.spacecraft = spacecraft
        self.instrument = instrument
        self.fetcher = fetcher

    async def process(self, item: Record, context: dict, **kwargs):
        self.logger.info(f"Downloading NOAA {self.spacecraft} {self.instrument} data.")

        downloaded = self.fetcher.download_csv(
            spacecraft=self.spacecraft,  # type: ignore
            instrument=self.instrument,  # type: ignore
        )

        if not downloaded:
            self.logger.info(
                f"No NOAA {self.spacecraft} {self.instrument} data downloaded."
            )
            return

        for file_path, path_handler in downloaded.items():
            content_date = path_handler.content_date  # type: ignore
            context[PROGRESS_DATE_CONTEXT_KEY] = content_date

            await self.publish_next(
                FileRecord(file_path, content_date),
                context,
                **kwargs,
            )
