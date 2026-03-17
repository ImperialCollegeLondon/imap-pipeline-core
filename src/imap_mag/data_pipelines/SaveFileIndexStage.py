from imap_db.model import FileIndex
from imap_mag.data_pipelines import Stage
from imap_mag.data_pipelines.Record import Record
from imap_mag.db import Database


class SaveFileIndexStage(Stage):
    """Stage that saves FileIndex objects to the database.

    Upserts using file_id as the unique key.
    """

    def __init__(self, database: Database | None):
        super().__init__()
        self.database = database

    async def process(self, item: Record, context: dict, **kwargs):
        file_index: FileIndex = getattr(item, "file_index")

        if self.database is None:
            self.logger.warning(
                "No database provided to SaveFileIndexStage, file index will not be saved"
            )
            await self.publish_next(item, context=context, **kwargs)
            return

        # Store values locally before upsert to avoid detached instance errors on log
        file_id = getattr(item, "file_id", None)
        record_count = file_index.record_count

        try:
            self.database.upsert_file_index(file_index)
            self.logger.info(
                f"Saved file index for file_id={file_id}, record_count={record_count}"
            )
        except Exception as e:
            self.logger.error(
                f"Failed to save file index for file_id={file_id}: {e}",
                exc_info=e,
            )

        await self.publish_next(item, context=context, **kwargs)
