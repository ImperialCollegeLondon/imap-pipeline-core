from datetime import datetime, timedelta
from pathlib import Path

from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from pydantic import SecretStr

from imap_mag.api.fetch.binary import fetch_binary
from imap_mag.api.process import process
from imap_mag.appConfig import create_serialize_config
from imap_mag.appUtils import HK_APIDS, forceUTCTimeZone, getPacketFromApID
from imap_mag.DB import Database
from imap_mag.outputManager import StandardSPDFMetadataProvider
from prefect_server.constants import CONSTANTS
from prefect_server.prefectUtils import get_secret_block


def convert_ints_to_string(apids: list[int]) -> str:
    return ",".join(str(apid) for apid in apids)


def generate_flow_run_name() -> str:
    parameters = flow_run.parameters
    hk_apids = parameters["hk_apids"]
    start_date = parameters["start_date"] or datetime.today().replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)
    end_date = parameters["end_date"] or start_date + timedelta(days=1)

    apid_text = (
        f"{convert_ints_to_string(hk_apids)}-ApIDs"
        if hk_apids != HK_APIDS
        else "all-HK"
    )

    return f"Download-{apid_text}-from-{start_date.strftime('%d-%m-%Y')}-to-{end_date.strftime('%d-%m-%Y')}"


@flow(
    name=CONSTANTS.FLOW_NAMES.POLL_HK,
    log_prints=True,
    flow_run_name=generate_flow_run_name,
)
async def poll_hk_flow(
    hk_apids: list[int] = HK_APIDS,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    auth_code: SecretStr | None = None,
):
    """
    Poll housekeeping data from WebPODA.
    """

    logger = get_run_logger()

    if not auth_code:
        auth_code = SecretStr(
            await get_secret_block(CONSTANTS.POLL_HK.WEBPODA_AUTH_CODE_SECRET_NAME)
        )

    if start_date is None:
        start_date = datetime.today().replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=1)

    if end_date is None:
        end_date = start_date + timedelta(days=1)

    (start_date, end_date) = forceUTCTimeZone(start_date, end_date)

    logger.info(
        f"Polling housekeeping data for ApIDs {convert_ints_to_string(hk_apids)} from {start_date.strftime('%d-%m-%Y')} to {end_date.strftime('%d-%m-%Y')}."
    )

    database = Database()

    for apid in hk_apids:
        packet_name = getPacketFromApID(apid)
        logger.debug(f"Downloading ApID {apid} ({packet_name}).")

        last_updated_date = database.get_download_progress_timestamp(packet_name)
        logger.debug(f"Last update for ApID {apid} is {last_updated_date}.")

        if (last_updated_date is None) or (last_updated_date <= start_date):
            logger.info(
                f"ApID {apid} is not up to date. Downloading from {start_date}."
            )
            actual_start_date = start_date
        elif last_updated_date >= end_date:
            logger.info(f"ApID {apid} is already up to date. Not downloading.")
            continue
        else:  # last_updated_date > start_date
            logger.info(
                f"ApID {apid} is partially up to date. Downloading from {last_updated_date}."
            )
            actual_start_date = last_updated_date

        downloaded_binaries: dict[Path, StandardSPDFMetadataProvider] = fetch_binary(
            auth_code=auth_code.get_secret_value(),
            apid=apid,
            start_date=actual_start_date,
            end_date=end_date,
        )

        for file, _ in downloaded_binaries.items():
            (_, config_file) = create_serialize_config(source=file.parent)
            process(file=Path(file.name), config=config_file)

        database.update_download_progress(packet_name, progress_timestamp=end_date)
