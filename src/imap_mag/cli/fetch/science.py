import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.client.SDCDataAccess import SDCDataAccess
from imap_mag.config import AppSettings, FetchMode
from imap_mag.download.FetchScience import FetchScience
from imap_mag.io import DatastoreFileManager
from imap_mag.io.file import SciencePathHandler
from imap_mag.util import MAGSensor, ReferenceFrame, ScienceLevel, ScienceMode

logger = logging.getLogger(__name__)


# E.g., imap-mag fetch science --start-date 2025-05-02 --end-date 2025-05-03
# E.g., imap-mag fetch science --ingestion-date --start-date 2025-05-02 --end-date 2025-05-03
def fetch_science(
    start_date: Annotated[datetime, typer.Option(help="Start date for the download")],
    end_date: Annotated[datetime, typer.Option(help="End date for the download")],
    use_ingestion_date: Annotated[
        bool,
        typer.Option(
            "--ingestion-date",
            help="Use ingestion date into SDC database, rather than science measurement date",
        ),
    ] = False,
    level: Annotated[
        ScienceLevel, typer.Option(case_sensitive=False, help="Level to download")
    ] = ScienceLevel.l2,
    reference_frames: Annotated[
        list[ReferenceFrame] | None,
        typer.Option(
            "--frame",
            case_sensitive=False,
            help="Reference frame to download for L2/L1D. Use None for L1B/L1C. Defaults to None - all reference frames.",
        ),
    ] = None,
    modes: Annotated[
        list[ScienceMode] | None,
        typer.Option(
            case_sensitive=False,
            help="Science modes to download. None = All modes",
        ),
    ] = None,
    sensors: Annotated[
        list[MAGSensor] | None,
        typer.Option(
            case_sensitive=False, help="Sensors to download. None = All sensors"
        ),
    ] = None,
    fetch_mode: Annotated[
        FetchMode,
        typer.Option(
            case_sensitive=False,
            help="Whether to download only or download and update progress in database",
        ),
    ] = FetchMode.DownloadOnly,
    max_downloads: Annotated[
        int | None,
        typer.Option(
            help="Maximum number of files to download. None means no limit.",
        ),
    ] = None,
    skip_items_count: Annotated[
        int,
        typer.Option(
            help="Number of items to skip from the start of the query results. Useful for batching downloads.",
        ),
    ] = 0,
) -> dict[Path, SciencePathHandler]:
    """Download science data from the SDC."""

    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.fetch_science)

    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    data_access = SDCDataAccess(
        auth_code=app_settings.fetch_science.api.auth_code,
        data_dir=work_folder,
        sdc_url=app_settings.fetch_science.api.url_base,
    )

    (modes, sensors, reference_frames) = _validate_and_complete_parameters(
        level, modes, sensors, reference_frames
    )

    fetch_science = FetchScience(data_access)
    downloaded_science: dict[Path, SciencePathHandler] = fetch_science.download_science(
        level=level,
        reference_frames=reference_frames,
        start_date=start_date,
        end_date=end_date,
        use_ingestion_date=use_ingestion_date,
        modes=modes,
        sensors=sensors,
        max_downloads=max_downloads,
        skip_items_count=skip_items_count,
    )

    if not downloaded_science:
        logger.info(
            f"No data downloaded for level {level.value} from {start_date} to {end_date}."
        )
    else:
        logger.debug(
            f"Downloaded {len(downloaded_science)} files:\n{', '.join(str(f) for f in downloaded_science.keys())}"
        )

    output_science: dict[Path, SciencePathHandler] = dict()

    if app_settings.fetch_science.publish_to_data_store:
        datastore_manager = DatastoreFileManager.CreateByMode(
            app_settings,
            use_database=(fetch_mode == FetchMode.DownloadAndUpdateProgress),
        )

        for file, path_handler in downloaded_science.items():
            (output_file, output_handler) = datastore_manager.add_file(
                file, path_handler
            )
            output_science[output_file] = output_handler

            # Clean up work folder files as have been copied to datastore
            logger.debug(f"Removing temporary file {file} from work folder.")
            file.unlink(missing_ok=False)
    else:
        logger.info("Files not published to data store based on config.")
        output_science = downloaded_science

    return output_science


def _validate_and_complete_parameters(
    level: ScienceLevel,
    modes: list[ScienceMode] | None,
    sensors: list[MAGSensor] | None,
    reference_frames: list[ReferenceFrame] | None,
) -> tuple[
    list[ScienceMode] | None, list[MAGSensor] | None, list[ReferenceFrame] | None
]:
    """
    Validate the parameters are correct and add defaults where needed to ensure the full set of downloads are completed

    If all are None then all files will be downloaded.
    If one of the descriptor components are specified (e.g. L1B mode but not sensor) then the missing components will be added as defaults to ensure a full set of files are downloaded.
    """

    # Normalize empty lists to None
    if sensors == [] or sensors == [None]:
        sensors = None
    if modes == [] or modes == [None]:
        modes = None
    if reference_frames == [] or reference_frames == [None]:
        reference_frames = None

    # If all are None then all files will be downloaded - simple case
    if not sensors and not modes and not reference_frames:
        return modes, sensors, reference_frames

    if level in [ScienceLevel.l1a, ScienceLevel.l1b, ScienceLevel.l1c]:
        if reference_frames:
            raise ValueError(
                f"Reference frames specified for level {level.value} which does not use reference frames"
            )

        if modes is not None and sensors is None:
            logger.info(
                f"Modes specified for level {level.value} but no sensors. Adding all default sensors."
            )
            sensors = [
                MAGSensor.IBS,
                MAGSensor.OBS,
            ]
        if sensors is not None and modes is None:
            logger.info(
                f"Sensors specified for level {level.value} but no modes. Adding all default modes."
            )
            modes = [
                ScienceMode.Normal,
                ScienceMode.Burst,
            ]

    if level in [ScienceLevel.l2, ScienceLevel.l1d]:
        if sensors is not None:
            raise ValueError(
                f"Sensors specified for level {level.value} which does not use sensors"
            )

        if modes is not None and reference_frames is None:
            logger.info(
                f"Modes specified for level {level.value} but no reference frames. Adding all default reference frames."
            )
            reference_frames = [
                ReferenceFrame.GSE,
                ReferenceFrame.DSRF,
                ReferenceFrame.SRF,
                ReferenceFrame.RTN,
                ReferenceFrame.GSM,
            ]

        if reference_frames is not None and modes is None:
            logger.info(
                f"Reference frames specified for level {level.value} but no modes. Adding all default modes."
            )
            modes = [
                ScienceMode.Normal,
                ScienceMode.Burst,
            ]

    return modes, sensors, reference_frames
