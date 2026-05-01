"""Download telemetry data from WebTCAD LaTiS API."""

import logging
from datetime import datetime
from enum import Enum, StrEnum

import requests
from pydantic import SecretStr

from imap_mag.config.FetchConfig import (
    FetchWebTCADLaTiSConfig,
)
from imap_mag.util.Subsystem import Subsystem

logger = logging.getLogger(__name__)

FLIGHT_SYSTEM_ID = "SID1"  # SID1 is flight, SID2 is preflight


class HKWebTCADItems(Enum):
    def __init__(
        self, tmid: int, packet_name: str, instrument: Subsystem, descriptor: str
    ) -> None:
        super().__init__()

        # Typer does not support Enums with tuple values,
        # so we need to overwrite the value with a string name
        self._value_ = self.name

        self.tmid = tmid
        self.packet_name = packet_name
        self.instrument = instrument
        self.descriptor = descriptor

    LO_PIVOT_PLATFORM_ANGLE = (
        58350,
        "ILOGLOBAL.PPM_NHK_POT_PRI",
        Subsystem.LO,
        "pivot-platform-angle",
    )
    HI45_ESA_STEP = (
        58238,
        "H45_APP_NHK.SCI_ESA_STEP",
        Subsystem.HI45,
        "esa-step",
    )
    HI90_ESA_STEP = (
        58309,
        "H90_APP_NHK.SCI_ESA_STEP",
        Subsystem.HI90,
        "esa-step",
    )


class WebTCADLaTiS:
    """Client for downloading telemetry data from the WebTCAD LaTiS API."""

    __auth_code: SecretStr
    __base_url: str

    class TimeQueryMode(StrEnum):
        SPACECRAFT_TIME_MODE = ""
        EARTH_RECEIVED_TIME_MODE = "_ERT"

    class ResultsFormat(StrEnum):
        CSV = "csv"
        JSON = "json"

    def __init__(self, fetch_webtcad_config: FetchWebTCADLaTiSConfig) -> None:
        self.__auth_code = fetch_webtcad_config.api.auth_code or SecretStr("")
        self.__base_url = fetch_webtcad_config.api.url_base.rstrip("/")

        if not self.__auth_code.get_secret_value():
            logger.warning(
                "No authentication code provided for WebTCAD LaTiS API. Requests may fail if authentication is required."
            )

        logger.info(
            f"WebTCAD LaTiS client initialized with base URL: {self.__base_url}"
        )

    def download_imap_lo_pivot_platform_angle_to_csv_file(
        self,
        *,
        start_date: datetime,
        end_date: datetime,
        system_id: str = FLIGHT_SYSTEM_ID,
        mode: TimeQueryMode = TimeQueryMode.SPACECRAFT_TIME_MODE,
    ) -> str:
        return self.download_analog_telemetry_item(
            telemetry_item_id=HKWebTCADItems.LO_PIVOT_PLATFORM_ANGLE.tmid,
            start_date=start_date,
            end_date=end_date,
            mode=mode,
            results_format=WebTCADLaTiS.ResultsFormat.CSV,
            system_id=system_id,
        )

    def download_analog_telemetry_item(
        self,
        *,
        telemetry_item_id: int,
        start_date: datetime,
        end_date: datetime,
        system_id: str = FLIGHT_SYSTEM_ID,
        mode: TimeQueryMode = TimeQueryMode.SPACECRAFT_TIME_MODE,
        results_format: ResultsFormat = ResultsFormat.CSV,
    ) -> str:
        """Download CSV data from the WebTCAD LaTiS API for a given telemetry item ID and date range.

        Args:
            telemetry_item_id: The telemetry item ID to query.
            start_date: Start of the time range (inclusive).
            end_date: End of the time range (exclusive).
            mode: The time query mode (spacecraft time or earth received time).
            results_format: The format of the results (CSV or JSON).
        Returns:
            The CSV content as a string.
        """

        if not system_id:
            raise ValueError("System ID must be provided.")

        if not start_date or not end_date:
            raise ValueError("Start date and end date must be provided.")

        if not telemetry_item_id:
            raise ValueError("Telemetry item ID must be provided.")

        start_str = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_str = end_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        url_path = (
            f"{self.__base_url.rstrip('/')}"
            "/AnalogTelemetryItem"
            f"{mode.value}_{system_id}.{results_format.value}"  # Assuming SID1 for flight data; adjust if needed
        )

        path_and_querystring = (
            f"{url_path}?TMID={telemetry_item_id}"
            f"&time,value"
            f"&time%3E={start_str}"
            f"&time%3C={end_str}"
            f"&format_time(yyyy-MM-dd'T'HH:mm:ss.SSS)"
        )

        headers = {
            "Authorization": f"Basic {self.__auth_code.get_secret_value()}",
        }

        logger.info(
            f"Downloading TMID {telemetry_item_id} from {start_date} to {end_date}."
        )

        try:
            logger.debug(f"GET {path_and_querystring}")
            response: requests.Response = requests.get(
                path_and_querystring,
                headers=headers,
            )
            logger.debug(f"Response status code: {response.status_code}")
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(
                f"Failed to download from WebTCAD LaTiS. GET {path_and_querystring} error",
                exc_info=e,
            )
            raise

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"Response contains {len(response.content)} bytes and {len(response.text.splitlines())} lines"
            )

        return response.text
