import logging

from imap_mag.config.AppSettings import AppSettings
from imap_mag.io import (
    DatastoreFileManager,
    DBIndexedDatastoreFileManager,
    IDatastoreFileManager,
)

logger = logging.getLogger(__name__)


def getManagerByMode(
    settings: AppSettings, use_database: bool
) -> IDatastoreFileManager:
    """Retrieve output manager based on destination and mode."""

    manager: IDatastoreFileManager = DatastoreFileManager(settings.data_store)

    if use_database:
        return DBIndexedDatastoreFileManager(manager, settings=settings)
    else:
        return manager
