from collections.abc import Callable
from typing import Any, Generic, TypeVar

from sqlalchemy.orm import InstrumentedAttribute

from imap_db.model import Base
from imap_mag.data_pipelines import Stage
from imap_mag.data_pipelines.Record import Record
from imap_mag.db import Database

TItem = TypeVar("TItem", bound=Base)


class GetDatabaseItemStage(Stage, Generic[TItem]):
    """Stage that looks up an existing database item for each Record and attaches it.

    The stage is generic over the type of database item (a SQLAlchemy model).
    It is made specific to an item type when the pipeline is set up:

    - ``item_type`` and ``key_column`` say which table and column to query.
    - ``get_key`` extracts the value to look up from the incoming Record.
    - ``set_item`` stores the item found (or None if there is no matching row)
      on the Record, which is then published to the next stage.

    Example:
        GetDatabaseItemStage[FileAnalysis](
            database=database,
            item_type=FileAnalysis,
            key_column=FileAnalysis.file_id,
            get_key=lambda record: record.file_id,
            set_item=lambda record, item: setattr(record, "file_analysis", item),
        )
    """

    def __init__(
        self,
        database: Database | None,
        item_type: type[TItem],
        key_column: InstrumentedAttribute,
        get_key: Callable[[Record], Any],
        set_item: Callable[[Record, TItem | None], None],
    ):
        super().__init__()
        self.database = database
        self.item_type = item_type
        self.key_column = key_column
        self.get_key = get_key
        self.set_item = set_item

    async def process(self, item: Record, context: dict, **kwargs):
        key = self.get_key(item)

        existing: TItem | None = None
        if self.database is not None and key is not None:
            existing = self.database.get_item_by_column(
                self.item_type, self.key_column, key
            )

        self.set_item(item, existing)

        await self.publish_next(item, context=context, **kwargs)
