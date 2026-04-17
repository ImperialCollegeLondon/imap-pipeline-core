from pathlib import Path

from imap_mag.client.SDCDataAccess import SDCDataAccess
from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import PROGRESS_DATE_CONTEXT_KEY, Record, Stage
from imap_mag.data_pipelines.Record import FileRecord
from imap_mag.db import Database
from imap_mag.io import DatastoreFileManager, IDatastoreFileManager
from imap_mag.io.file.SpinTablePathHandler import SpinTablePathHandler
from imap_mag.util.Humaniser import Humaniser
from imap_mag.util.TimeConversion import TimeConversion


class DownloadSpinTableFilesStage(Stage):
    """Downloads spin table files from the SDC API and publishes them to the datastore.

    This stage combines download and publish because spin table metadata from the API
    (start_date, end_date, version, ingestion_date) needs to be indexed in the database,
    and this metadata is only available at download time.
    """

    def __init__(
        self,
        client: SDCDataAccess,
        settings: AppSettings,
        database: Database | None,
    ):
        super().__init__()
        self.client = client
        self.settings = settings
        self.database = database

    def prepare(self, run_parameters, next_stage, index):
        self.work_folder = self.settings.setup_work_folder_for_command(
            self.settings.fetch_spice
        )
        return super().prepare(run_parameters, next_stage, index)

    async def process(self, item: Record, context: dict, **kwargs):
        if not item or not item.start_date or not item.end_date:
            raise ValueError(
                "DownloadSpinTableFilesStage requires a Record with start_date and end_date"
            )

        start_date = item.start_date
        end_date = item.end_date

        self.logger.info(
            f"Querying spin table files from {start_date} to {end_date}..."
        )

        spin_files = self.client.spin_table_query(
            start_ingest_date=start_date.date()
            if hasattr(start_date, "date")
            else start_date,
            end_ingest_date=end_date.date() if hasattr(end_date, "date") else end_date,
        )

        if not spin_files:
            self.logger.info("No spin table files found for the given date range.")
            return

        self.logger.info(f"Found {len(spin_files)} spin table files to download.")

        output_manager: IDatastoreFileManager | None = None
        if self.settings.fetch_spice.publish_to_data_store:
            output_manager = DatastoreFileManager.CreateByMode(
                self.settings,
                use_database=self.database is not None,
                database=self.database,
            )

        for file_meta in spin_files:
            file_path_str = file_meta.get("file_path")
            if not file_path_str:
                self.logger.warning(
                    f"Spin table entry missing file_path: {file_meta}. Skipping."
                )
                continue

            ingestion_date = TimeConversion.try_extract_iso_like_datetime(
                file_meta, "ingestion_date"
            )

            # Filter: skip files ingested before start_date
            if ingestion_date and ingestion_date <= start_date:
                self.logger.info(
                    f"Skipped {file_path_str} as ingestion_date {ingestion_date} is before start date {start_date}."
                )
                continue

            downloaded_file: Path = self.client.download_spin_table(file_path_str)
            file_size = downloaded_file.stat().st_size

            if file_size == 0:
                self.logger.warning(
                    f"Downloaded file {downloaded_file} is empty. Skipping."
                )
                continue

            self.logger.info(
                f"Downloaded {Humaniser.format_bytes(file_size)} {downloaded_file}"
            )

            handler = SpinTablePathHandler.from_filename(downloaded_file)
            if handler is None:
                self.logger.error(
                    f"Could not parse {downloaded_file} into SpinTablePathHandler. Skipping."
                )
                continue

            handler.add_metadata(file_meta)

            # Publish to datastore with metadata preserved
            output_file = downloaded_file
            if output_manager is not None:
                (output_file, _) = output_manager.add_file(downloaded_file, handler)

            # Update progress to the latest ingestion date
            if ingestion_date:
                context[PROGRESS_DATE_CONTEXT_KEY] = ingestion_date

            content_date = handler.get_content_date_for_indexing()
            if content_date is None:
                self.logger.warning(
                    f"Could not determine content date for {downloaded_file}. Using ingestion_date."
                )
                content_date = ingestion_date

            await self.publish_next(
                FileRecord(output_file, content_date),
                context,
                **kwargs,
            )
