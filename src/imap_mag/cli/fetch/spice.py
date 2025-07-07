import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appUtils
from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.client.SDCDataAccess import SDCDataAccess
from imap_mag.config import AppSettings, FetchMode
from imap_mag.download.FetchSPICE import FetchSPICE
from imap_mag.io import SPICEPathHandler
from imap_mag.util import SPICEType

logger = logging.getLogger(__name__)


# E.g., imap-mag fetch spice --start-date 2025-05-02 --end-date 2025-05-03
# E.g., imap-mag fetch spice --ingestion-date --start-date 2025-05-02 --end-date 2025-05-03
def fetch_spice(
    start_date: Annotated[datetime, typer.Option(help="Start date for the download")],
    end_date: Annotated[datetime, typer.Option(help="End date for the download")],
    use_ingestion_date: Annotated[
        bool,
        typer.Option(
            "--ingestion-date",
            help="Use ingestion date into SDC database, rather than spice measurement date",
        ),
    ] = False,
    types: Annotated[
        list[SPICEType],
        typer.Option(case_sensitive=False, help="SPICE types to download"),
    ] = [s for s in SPICEType],
    fetch_mode: Annotated[
        FetchMode,
        typer.Option(
            case_sensitive=False,
            help="Whether to download only or download and update progress in database",
        ),
    ] = FetchMode.DownloadOnly,
    auth_code: Annotated[
        str | None,
        typer.Option(
            envvar="SDC_AUTH_CODE",
            help="IMAP Science Data Centre API Key",
        ),
    ] = None,
) -> dict[Path, SPICEPathHandler]:
    """Download spice data from the SDC."""

    # "auth-code" is usually defined in the config file but the CLI allows for it to
    # be specified on the command cli with "--auth-code" or in an env vars:
    # SDC_AUTH_CODE or MAG_FETCH_SPICE_API_AUTH_CODE
    settings_overrides = (
        {"fetch_spice": {"api": {"auth_code": auth_code}}} if auth_code else {}
    )

    app_settings = AppSettings(**settings_overrides)  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.fetch_spice)
    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    data_access = SDCDataAccess(
        data_dir=work_folder,
        sdc_url=app_settings.fetch_spice.api.url_base,
    )

    fetch_spice = FetchSPICE(data_access, types=types)
    downloaded_spice: dict[Path, SPICEPathHandler] = fetch_spice.download_spice(
        start_date=start_date,
        end_date=end_date,
        use_ingestion_date=use_ingestion_date,
    )

    if not downloaded_spice:
        logger.info(
            f"No data downloaded for SPICE types {', '.join([t.value for t in types])} from {start_date} to {end_date}."
        )
    else:
        logger.debug(
            f"Downloaded {len(downloaded_spice)} files:\n{', '.join(str(f) for f in downloaded_spice.keys())}"
        )

    output_spice: dict[Path, SPICEPathHandler] = dict()

    if app_settings.fetch_spice.publish_to_data_store:
        output_manager = appUtils.getOutputManagerByMode(
            app_settings.data_store,
            use_database=(fetch_mode == FetchMode.DownloadAndUpdateProgress),
        )

        for file, path_handler in downloaded_spice.items():
            (output_file, output_handler) = output_manager.add_file(file, path_handler)
            output_spice[output_file] = output_handler
    else:
        logger.info("Files not published to data store based on config.")

    return output_spice
