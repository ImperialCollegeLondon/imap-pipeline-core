"""Program to retrieve and process MAG binary files."""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from imap_mag.client.webPODA import WebPODA
from imap_mag.io import StandardSPDFMetadataProvider
from imap_mag.util import HKLevel

logger = logging.getLogger(__name__)


@dataclass
class WebPODAMetadataProvider(StandardSPDFMetadataProvider):
    """
    Metadata for WebPODA binaries.
    """

    ert: datetime | None = None  # date data was received by WebPODA


class FetchBinary:
    """Manage WebPODA data."""

    __MAG_PREFIX: str = "mag_"

    __web_poda: WebPODA

    def __init__(
        self,
        web_poda: WebPODA,
    ) -> None:
        """Initialize WebPODA interface."""

        self.__web_poda = web_poda

    def download_binaries(
        self,
        packet: str,
        start_date: datetime,
        end_date: datetime,
        use_ert: bool = False,
    ) -> dict[Path, WebPODAMetadataProvider]:
        """Retrieve WebPODA data."""

        downloaded: dict[Path, WebPODAMetadataProvider] = dict()

        if start_date == end_date:
            # If the start and end dates are the same, download all the data from that day.
            start = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            dates: list[datetime] = [start, start + timedelta(days=1)]
        else:
            # Download all the data from the start date to the end date for each day separately.
            # Force the date ranges to include midnights to avoid missing data.
            dates = (
                pd.date_range(
                    start=start_date.replace(hour=0, minute=0, second=0, microsecond=0),
                    end=end_date.replace(hour=0, minute=0, second=0, microsecond=0)
                    + timedelta(days=1),
                    freq="D",
                    normalize=True,
                    inclusive="both",
                )
                .to_pydatetime()
                .tolist()
            )

            # Remove any data outside of bounds, and forcibly re-add the start and end dates.
            dates[:] = [x for x in dates if start_date < x < end_date]
            dates = [start_date, *dates, end_date]

            # If the end date is at midnight, it means we want to download that full day, too.
            # So include the next midnight as the end date.
            if end_date == end_date.replace(hour=0, minute=0, second=0, microsecond=0):
                dates.append(
                    end_date + timedelta(days=1),
                )

        # Download the data in chunks of 1 day.
        for d in range(len(dates) - 1):
            file = self.__web_poda.download(
                packet=packet,
                start_date=dates[d],
                end_date=dates[d + 1],
                ert=use_ert,
            )

            if file.stat().st_size > 0:
                logger.info(f"Downloaded file from WebPODA: {file}")

                max_ert: datetime | None = self.__web_poda.get_max_ert(
                    packet=packet,
                    start_date=dates[d],
                    end_date=dates[d + 1],
                    ert=use_ert,
                )
                min_time: datetime | None = self.__web_poda.get_min_sctime(
                    packet=packet,
                    start_date=dates[d],
                    end_date=dates[d + 1],
                    ert=use_ert,
                )

                downloaded[file] = WebPODAMetadataProvider(
                    level=HKLevel.l0.value,
                    descriptor=f"{packet.lower().strip(self.__MAG_PREFIX).replace('_', '-')}",
                    content_date=(
                        min_time.replace(hour=0, minute=0, second=0)
                        if min_time
                        else None
                    ),
                    ert=max_ert,
                    extension="pkts",
                )
            else:
                logger.debug(f"Downloaded file {file} is empty and will not be used.")

        return downloaded
