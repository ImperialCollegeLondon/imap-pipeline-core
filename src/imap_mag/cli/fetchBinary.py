"""Program to retrieve and process MAG binary files."""

import typing
from datetime import datetime
from pathlib import Path

import pandas as pd
import typing_extensions

from ..client.webPODA import WebPODA
from ..outputManager import IOutputManager


class FetchBinaryOptions(typing.TypedDict):
    """Options for WebPODA interactions."""

    packet: str
    start_date: datetime
    end_date: datetime


class FetchBinary:
    """Manage WebPODA data."""

    __MAG_PREFIX: str = "mag_"

    __web_poda: WebPODA
    __output_manager: IOutputManager | None

    def __init__(
        self,
        web_poda: WebPODA,
        output_manager: IOutputManager | None = None,
    ) -> None:
        """Initialize WebPODA interface."""

        self.__web_poda = web_poda
        self.__output_manager = output_manager

    def download_binaries(
        self, **options: typing_extensions.Unpack[FetchBinaryOptions]
    ) -> list[Path]:
        """Retrieve WebPODA data."""

        downloaded = []

        date_range: pd.DatetimeIndex = pd.date_range(
            start=options["start_date"],
            end=options["end_date"],
            freq="D",
            normalize=True,
        )

        dates = date_range.to_pydatetime().tolist()
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
                if self.__output_manager is not None:
                    self.__output_manager.add_default_format_file(
                        file,
                        descriptor=options["packet"]
                        .lower()
                        .strip(self.__MAG_PREFIX)
                        .replace("_", "-"),
                        date=dates[d],
                        extension="pkts",
                    )

                downloaded += [file]

        return downloaded
