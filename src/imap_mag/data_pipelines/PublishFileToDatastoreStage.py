from pathlib import Path

from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import FileRecord, Record, Stage
from imap_mag.db import Database
from imap_mag.io import FilePathHandlerSelector
from imap_mag.io.DatastoreFileManager import DatastoreFileManager


class PublishFileToDatastoreStage(Stage):
    def __init__(
        self,
        enabled: bool,
        database: Database | None,
        settings: AppSettings = AppSettings(),
    ):
        super().__init__()
        self.enabled = enabled
        self.database = database
        self.settings = settings

        self.datastore_manager = DatastoreFileManager.CreateByMode(
            self.settings,
            use_database=self.database is not None,
            database=self.database,
        )

    async def process(self, item: Record, context: dict, **kwargs):
        if not self.enabled:
            await self.publish_next(item, context, **kwargs)
            return

        if not hasattr(item, "file_path"):
            raise ValueError(
                "PublishFileToDatastoreStage expects items with a file_path attribute"
            )

        file_path: Path = item.file_path

        if not file_path.exists():
            raise ValueError(
                f"File {file_path} does not exist, cannot publish to datastore"
            )

        path_handler = FilePathHandlerSelector.find_by_path(file_path)

        content_date = path_handler.get_content_date_for_indexing()

        if content_date is None:
            raise ValueError(
                f"Could not determine content date for file {file_path}, cannot publish to datastore"
            )

        saved_path, _ = self.datastore_manager.add_file(file_path, path_handler)

        await self.publish_next(FileRecord(saved_path, content_date), context, **kwargs)
