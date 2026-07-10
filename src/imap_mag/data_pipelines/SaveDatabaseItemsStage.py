from imap_db.model import Base
from imap_mag.data_pipelines import Stage
from imap_mag.data_pipelines.Record import Record
from imap_mag.db import Database


class SaveDatabaseItemsStage(Stage):
    """Stage that saves any SQLAlchemy model instances found in a Record to the database.

    All attributes on each Record that are instances of the SQLAlchemy
    ``Base`` declarative base are persisted via ``database.save()``, which
    uses ``session.merge()`` under the hood and therefore handles both
    inserts (no primary key set) and updates (primary key already exists).
    """

    def __init__(self, database: Database | None):
        super().__init__()
        self.database = database

    async def process(self, item: Record, context: dict, **kwargs):
        if self.database is None:
            self.logger.warning(
                "No database provided to SaveDatabaseItemsStage, items will not be saved"
            )
            await self.publish_next(item, context=context, **kwargs)
            return

        for value in vars(item).values():
            if isinstance(value, Base):
                self.database.save(value)

        await self.publish_next(item, context=context, **kwargs)
