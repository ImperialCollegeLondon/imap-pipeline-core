"""Program to retrieve and process MAG CDF files."""

import logging
from datetime import datetime
from pathlib import Path

from imap_mag.client.SDCDataAccess import SDCDataAccess
from imap_mag.io.file import SpicePathHandler
from imap_mag.util import SpiceType

logger = logging.getLogger(__name__)


class FetchSpice:
    """Fetch SPICE data from SDC."""

    __data_access: SDCDataAccess

    __types: list[SpiceType]

    def __init__(
        self,
        data_access: SDCDataAccess,
        types: list[SpiceType] = [s for s in SpiceType],
    ) -> None:
        """Initialize SDC interface."""

        self.__data_access = data_access
        self.__types = types

    def download_spice(
        self,
        start_date: datetime,
        end_date: datetime,
        use_ingestion_date: bool = False,
    ) -> dict[Path, SpicePathHandler]:
        """Retrieve SDC data."""

        downloaded: dict[Path, SpicePathHandler] = dict()

        dates: dict[str, datetime] = {
            "ingestion_start_date" if use_ingestion_date else "start_date": start_date,
            "ingestion_end_date" if use_ingestion_date else "end_date": end_date,
        }

        for type in self.__types:
            file_details = self.__data_access.get_filename(
                table="spice",
                descriptor=type.value,
                **dates,  # type: ignore
            )

            if file_details is not None:
                for file in file_details:
                    downloaded_file: Path = self.__data_access.download(
                        file["file_path"]
                    )
                    spice_handler: SpicePathHandler | None = (
                        SpicePathHandler.from_filename(downloaded_file)
                    )

                    if spice_handler and downloaded_file.stat().st_size > 0:
                        logger.info(
                            f"Downloaded SPICE file from SDC: {downloaded_file}"
                        )
                        downloaded[downloaded_file] = spice_handler
                    else:
                        logger.debug(
                            f"Downloaded file {downloaded_file} is empty and will not be used."
                        )

        return downloaded
