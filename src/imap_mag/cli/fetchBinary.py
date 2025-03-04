"""Program to retrieve and process MAG binary files."""

import logging
import typing
from datetime import datetime
from pathlib import Path

import pandas as pd
import typing_extensions

from imap_mag.client.webPODA import WebPODA
from imap_mag.outputManager import StandardSPDFMetadataProvider


class FetchBinaryOptions(typing.TypedDict):
    """Options for WebPODA interactions."""

    packet: str
    start_date: datetime
    end_date: datetime


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
        self, **options: typing_extensions.Unpack[FetchBinaryOptions]
    ) -> dict[Path, StandardSPDFMetadataProvider]:
        """Retrieve WebPODA data."""

        downloaded: dict[Path, StandardSPDFMetadataProvider] = dict()

        date_range: pd.DatetimeIndex = pd.date_range(
            start=options["start_date"],
            end=options["end_date"],
            freq="D",
            normalize=True,
        )
        dates: list[datetime] = date_range.to_pydatetime().tolist()

        if len(dates) == 1:
            dates += [
                pd.Timestamp(dates[0] + pd.Timedelta(days=1))
                .normalize()
                .to_pydatetime()
            ]

        for d in range(len(dates) - 1):
            file: Path = self.__web_poda.download(
                packet=options["packet"], start_date=dates[d], end_date=dates[d + 1]
            )

            if file.stat().st_size > 0:
                logging.info(f"Downloaded file from WebPODA: {file}")

                downloaded[file] = StandardSPDFMetadataProvider(
                    descriptor=options["packet"]
                    .lower()
                    .strip(self.__MAG_PREFIX)
                    .replace("_", "-"),
                    date=dates[d],
                    extension="pkts",
                )
            else:
                logging.debug(f"Downloaded file {file} is empty and will not be used.")

        return downloaded
