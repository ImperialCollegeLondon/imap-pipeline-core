"""Program to retrieve SPICE kernel files from SDC."""

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from typing import Annotated

import spiceypy
import typer

from imap_db.model import File
from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.client.SDCDataAccess import SDCDataAccess
from imap_mag.config import AppSettings
from imap_mag.db import Database
from imap_mag.io import DatastoreFileManager, IDatastoreFileManager
from imap_mag.io.file import SPICEPathHandler
from imap_mag.io.file.SPICEPathHandler import METAKERNEL_FILENAME_PREFIX
from imap_mag.process.metakernel import MetaKernel
from imap_mag.util import DatetimeProvider
from imap_mag.util.Humaniser import Humaniser
from imap_mag.util.TimeConversion import TimeConversion

logger = logging.getLogger(__name__)

SPICE_FILE_META_FIELD_INGESTION_DATE = "ingestion_date"
SPICE_FILE_META_FIELD_KERNAL_TYPE = "kernel_type"

# taken from https://github.com/IMAP-Science-Operations-Center/sds-data-manager/blob/dev/sds_data_manager/lambda_code/SDSCode/pipeline_lambdas/spice_indexer.py#L77C1-L86C1
minimum_mission_time = datetime(2010, 1, 1)
maximum_mission_time = datetime(2145, 1, 1)
MAXIMUM_DATETIME_INTERVAL = [[minimum_mission_time, maximum_mission_time]]
MAXIMUM_SCLK_INTERVAL = [
    ["1/0000000000:00000", "1/4260211203:00000"]
]  # Calculated from the above datetimes seperately
MAXIMUM_J2000_INTERVAL = [
    [315576066.1839245, 4575787269.183866]
]  # Calculated from the above datetimes seperately
METAKERNEL_FOLDER = "mk"

"""
Example SPICE file metadata from SDC API:
GET https://api.imap-mission.com/spice-query?start_ingest_date=20251101&end_ingest_date=20251105
    [{
        "file_name": "ck/imap_2025_302_2025_303_001.ah.bc",
        "file_root": "imap_2025_302_2025_303_.ah.bc",
        "kernel_type": "attitude_history",
        "version": 1,
        "min_date_j2000": 815036897.0909909,
        "max_date_j2000": 815126896.0094784,
        "file_intervals_j2000": [
            [
                815036897.0909909,
                815126896.0094784
            ]
        ],
        "min_date_datetime": "2025-10-29, 19:07:07",
        "max_date_datetime": "2025-10-30, 20:07:06",
        "file_intervals_datetime": [
            [
                "2025-10-29T19:07:07.908503+00:00",
                "2025-10-30T20:07:06.826978+00:00"
            ]
        ],
        "min_date_sclk": "1/0499460830:00000",
        "max_date_sclk": "1/0499550829:00000",
        "file_intervals_sclk": [
            [
                "1/0499460830:00000",
                "1/0499550829:00000"
            ]
        ],
        "sclk_kernel": "/tmp/naif0012.tls",
        "lsk_kernel": "/tmp/imap_sclk_0031.tsc",
        "ingestion_date": "2025-11-01, 08:05:12",
        "timestamp": 1761984312.0
    }...]

"""


# Kernel types code from IMAP SDC. Copyright Colorado Unitersite
# See https://raw.githubusercontent.com/IMAP-Science-Operations-Center/sds-data-manager/refs/heads/dev/sds_data_manager/lambda_code/SDSCode/api_lambdas/spice_metakernel_api.py
class LeapsecondKernels(Enum):
    """Container for Leapsecond Kernel Types."""

    LEAPSECONDS = auto()

    @staticmethod
    def spice_category_name():
        """Category of SPICE file."""
        return "leapseconds_category"


class PlanetaryConstantsKernels(Enum):
    """Container for Planetary Contants Kernel Types."""

    PLANETARY_CONSTANTS = auto()

    @staticmethod
    def spice_category_name():
        """Category of SPICE file."""
        return "planetary_constants_category"


class ScienceFramesKernels(Enum):
    """Container for Science Frames Kernel Type."""

    SCIENCE_FRAMES = auto()

    @staticmethod
    def spice_category_name():
        """Category of SPICE file."""
        return "science_frames_category"


class IMAPFramesKernels(Enum):
    """Container for IMAP Frames Kernel Type."""

    IMAP_FRAMES = auto()

    @staticmethod
    def spice_category_name():
        """Category of SPICE file."""
        return "imap_frames_category"


class SpacecraftClockKernels(Enum):
    """Container for Spacecraft Clock Kernel Types."""

    SPACECRAFT_CLOCK = auto()

    @staticmethod
    def spice_category_name():
        """Category of SPICE file."""
        return "spacecraft_clock_category"


class PlanetaryEphemerisKernels(Enum):
    """Container for Planetary Ephemeris Kernel Types."""

    PLANETARY_EPHEMERIS = auto()

    @staticmethod
    def spice_category_name():
        """Category of SPICE file."""
        return "planetary_ephemeris_category"


class SpacecraftEphemerisKernels(Enum):
    """Container for Spacecraft Ephemeris Kernel Types."""

    EPHEMERIS_RECONSTRUCTED = auto()
    EPHEMERIS_NOMINAL = auto()
    EPHEMERIS_PREDICTED = auto()
    EPHEMERIS_90DAYS = auto()
    EPHEMERIS_LONG = auto()
    EPHEMERIS_LAUNCH = auto()

    @staticmethod
    def spice_category_name():
        """Category of SPICE file."""
        return "spacecraft_ephemeris_category"


class SpacecraftAttitudeKernels(Enum):
    """Container for Spacecraft Attitude Kernel Types."""

    ATTITUDE_HISTORY = auto()
    ATTITUDE_PREDICT = auto()

    @staticmethod
    def spice_category_name():
        """Category of SPICE file."""
        return "spacecraft_attitude_category"


class EarthAttitudeKernels(Enum):
    """Container for Earth Attitude Kernel Types."""

    EARTH_ATTITUDE = auto()

    @staticmethod
    def spice_category_name():
        """Category of SPICE file."""
        return "earth_attitude_category"


class PointingAttitudeKernels(Enum):
    """Container for Pointing Attitude Kernel Types."""

    POINTING_ATTITUDE = auto()

    @staticmethod
    def spice_category_name():
        """Category of SPICE file."""
        return "pointing_attitude_category"


@dataclass
class KernelCollection:
    """Collection of SPICE kernel types for IMAP."""

    imap_spice_load_order: list = field(
        default_factory=lambda: [
            LeapsecondKernels,
            PlanetaryConstantsKernels,
            IMAPFramesKernels,
            ScienceFramesKernels,
            SpacecraftClockKernels,
            EarthAttitudeKernels,
            PlanetaryEphemerisKernels,
            SpacecraftEphemerisKernels,
            SpacecraftAttitudeKernels,
            PointingAttitudeKernels,
        ]
    )

    @property
    def file_types(self):
        """Return all kernel members in lowercase."""
        members = []
        for kernel_class in self.imap_spice_load_order:
            members.extend([member.name.lower() for member in kernel_class])
        return members

    @property
    def category_types(self):
        """Collect all kernel category type strings."""
        return [
            kernel_class.spice_category_name()
            for kernel_class in self.imap_spice_load_order
        ]


# E.g. IMAP_API_KEY=KEY_HERE IMAP_DATA_ACCESS_URL=https://api.imap-mission.com/api-key imap-mag fetch spice --ingest-start-day 2025-11-01 --ingest-end-date 2025-11-05 --latest
#      imap-mag fetch spice --ingest-start-day 2025-11-01 --ingest-end-date 2025-11-05 --use-database
def fetch_spice(
    ingest_start_day: datetime | None = None,
    ingest_end_date: datetime | None = None,
    file_name: str | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
    kernel_type: str | None = None,
    latest: bool = False,
    use_database: bool = False,
) -> list[tuple[Path, SPICEPathHandler, dict[str, str]]]:
    """Download SPICE kernel files from the SDC.

    Args:
        data_access: SDCDataAccess instance for API communication
        ingest_start_day: Start date for ingestion date filter
        ingest_end_date: End date for ingestion date filter (exclusive)
        file_name: Spice kernel file name filter
        start_time: Coverage start time in TDB seconds
        end_time: Coverage end time in TDB seconds
        kernel_type: Spice kernel type filter
        latest: If True, only return latest version of kernels matching query

    Returns:
        Dictionary of the downloaded file paths, and the key value metadata from the SDC
    """

    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.fetch_science)
    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)
    data_access = SDCDataAccess(
        auth_code=app_settings.fetch_spice.api.auth_code,
        data_dir=work_folder,
        sdc_url=app_settings.fetch_spice.api.url_base,
    )

    # Query SPICE files from SDC
    spice_file_query_results = data_access.spice_query(
        ingest_start_day=ingest_start_day.date() if ingest_start_day else None,
        ingest_end_date=ingest_end_date.date() if ingest_end_date else None,
        file_name=file_name,
        start_time=start_time,
        end_time=end_time,
        kernel_type=kernel_type,
        latest=latest,
    )

    if not spice_file_query_results:
        logger.info("No SPICE files found matching the query criteria")
        return []

    logger.info(f"Found {len(spice_file_query_results)} SPICE files to download")

    downloaded_spice_files_and_meta = download_spice_files_later_than(
        data_access, ingest_start_day, spice_file_query_results
    )

    output_manager: IDatastoreFileManager | None = None
    if not app_settings.fetch_spice.publish_to_data_store:
        logger.info("Files not published to data store based on config.")
    else:
        output_manager = DatastoreFileManager.CreateByMode(
            app_settings,
            use_database=use_database,
        )

    output_spice: list[tuple[Path, SPICEPathHandler, dict[str, str]]] = []

    for file_path, file_metadata in downloaded_spice_files_and_meta.items():
        handler = SPICEPathHandler.from_filename(file_path)
        if handler is None:
            logger.error(
                f"Downloaded SPICE file {file_path} could not be parsed into SPICEPathHandler. Skipping publish to data store."
            )
            continue

        handler.add_metadata(file_metadata)
        if output_manager is not None:
            (output_file, output_handler) = output_manager.add_file(file_path, handler)
            output_spice.append((output_file, output_handler, file_metadata))
        else:
            output_spice.append((file_path, handler, file_metadata))

    return output_spice


def download_spice_files_later_than(
    data_access: SDCDataAccess,
    ingest_start_day: datetime | None,
    spice_file_query_results,
) -> dict[Path, dict[str, str]]:
    downloaded: dict[Path, dict[str, str]] = {}

    for file_meta in [
        f for f in spice_file_query_results if f["file_name"] is not None
    ]:
        file_was_ingested = TimeConversion.try_extract_iso_like_datetime(
            file_meta, SPICE_FILE_META_FIELD_INGESTION_DATE
        )
        if (
            file_was_ingested
            and ingest_start_day
            and file_was_ingested <= ingest_start_day
        ):
            logger.info(
                f"Skipped {file_meta['file_name']} as SDC ingestion_date {file_was_ingested}, before start date {ingest_start_day}. "
            )
            continue

        downloaded_file = data_access.download(file_meta["file_name"])  # type: ignore
        file_size = downloaded_file.stat().st_size
        if file_size > 0:
            logger.info(
                f"Downloaded {Humaniser.format_bytes(file_size)} {downloaded_file}"
            )
            downloaded[downloaded_file] = file_meta
        else:
            logger.warning(
                f"Downloaded file {downloaded_file} is empty and will not be used."
            )

    logger.info(f"{len(downloaded)} SPICE files downloaded")

    return downloaded


# E.g. imap-mag fetch metakernel --start-time 2025-11-01T00:00:00 --end-time 2025-11-05T23:59:59 --output-path ./metakernels --file-types ck,spk --publish-to-datastore
#      imap-mag fetch metakernel --start-time 2025-11-01T00:00:00 --end-time 2025-11-05T23:59:59 --publish-to-datastore
#      imap-mag fetch metakernel --start-time 2025-11-01T00:00:00 --end-time 2025-11-05T23:59:59 --list-files
def generate_spice_metakernel(
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    output_path: Path | None = None,
    file_types: list[str] | None = None,
    publish_to_datastore: Annotated[
        bool, typer.Option("--publish/--no-publish", "-p")
    ] = False,
    list_files: Annotated[bool, typer.Option("--list", "-l")] = False,
    require_coverage: Annotated[
        bool, typer.Option("--require-coverage/--no-require-coverage", "-c")
    ] = False,
    verify: Annotated[bool, typer.Option("--verify/--no-verify")] = True,
    base_path: Path | None = None,
) -> Path | list[Path]:
    """Generate a SPICE metakernel file for the downloaded SPICE kernels.

    Args:
        start_time: Coverage start time
        end_time: Coverage end time
        output_path: folder in which to save the generated metakernel file. If None the automated naming will be used. Cannot use with publish_to_datastore
        file_types: Set of file types to include in the metakernel. If None, all types are included.
        publish_to_datastore: Whether to publish the generated metakernel to the data store in the spice/mk folder. Cannot use with output_path
        database: Database instance to use for retrieving SPICE files. If None, a new instance will be created.
        list_files: If True, return list of files in metakernel instead of generating the metakernel file.

    Returns:
        Path to the generated metakernel file.
    """
    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.fetch_science)
    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)
    database = Database()

    if file_types:
        file_types = [type.strip().upper() for type in file_types]

    if output_path and publish_to_datastore:
        raise ValueError(
            "Cannot specify both output_path and publish_to_datastore - use one or other to avoid confusion about where the metakernel will be saved and paths in it."
        )

    if publish_to_datastore and list_files:
        raise ValueError("Cannot both publish metakernel and list files.")

    # get all SPICE files from the database except MK files
    mk_folder = SPICEPathHandler.get_root_folder() + os.path.sep + METAKERNEL_FOLDER
    files = database.get_files_by_path(
        SPICEPathHandler.get_root_folder(), ~File.path.startswith(mk_folder)
    )  # type: ignore

    if not files:
        logger.warning("No SPICE files found in the database to include in metakernel.")
        raise RuntimeError("No SPICE files found in the database.")

    logger.debug(f"Queried {len(files)} files from DB")

    metakernel = _metakernel_builder(
        start_time,
        end_time,
        files,
        spice_folder=app_settings.data_store,
        file_types=set(file_types) if file_types else None,
    )

    start_time = TimeConversion.j2000_to_datetime(int(metakernel.start_time_j2000))
    end_time = TimeConversion.j2000_to_datetime(int(metakernel.end_time_j2000))

    assert start_time is not None
    assert end_time is not None

    metakernel_file_path: Path = (
        work_folder
        / mk_folder
        / f"{METAKERNEL_FILENAME_PREFIX}_{start_time.strftime('%Y%m%dT%H%M%S')}_{end_time.strftime('%Y%m%dT%H%M%S')}_v001.tm"
    )

    if (require_coverage) and metakernel.contains_gaps():
        raise RuntimeError("Metakernel cannot be generated due to gaps in SPICE")
    elif metakernel.contains_gaps():
        logger.warning(
            "Generated metakernel contains gaps in SPICE coverage. Use --require-coverage flag to raise an error instead."
        )

    if list_files:
        metakernel_files = metakernel.return_spice_files_in_order(detailed=False)
        if not metakernel_files:
            raise RuntimeError("Zero SPICE kernels found")
        output = [Path(f) for f in metakernel_files]

        logger.debug(f"SPICE files in metakernel: {output}")

        return output
    else:
        if publish_to_datastore:
            output_path = (
                app_settings.data_store
                / SPICEPathHandler.get_root_folder()
                / METAKERNEL_FOLDER
            )

        kernel_contents = metakernel.return_tm_file(base_path=base_path or Path("./"))

        if metakernel_file_path.parent.is_dir() is False:
            metakernel_file_path.parent.mkdir(parents=True, exist_ok=True)

        abs_metakernal_path = str(metakernel_file_path.resolve())
        with open(metakernel_file_path, "w") as metakernel_file:
            metakernel_file.write(kernel_contents)
            logger.info(f"Generated SPICE metakernel file at {abs_metakernal_path}")

        if verify:
            original_cwd = Path.cwd()
            os.chdir(app_settings.data_store)
            try:
                spiceypy.kclear()
                spiceypy.furnsh(abs_metakernal_path)
                spiceypy.kclear()
                logger.info(
                    f"Verified generated metakernel can be furnished in SPICE from base {app_settings.data_store}"
                )
            finally:
                os.chdir(original_cwd)
        else:
            logger.info("Skipping verification of generated metakernel")

        should_copy_file_to_output_folder = (
            output_path is not None
            and not publish_to_datastore
            and metakernel_file_path.parent.resolve() != output_path.resolve()
        )
        if should_copy_file_to_output_folder:
            assert output_path is not None  # Type narrowing for mypy/pylance
            final_metakernel_path = output_path / metakernel_file_path.name

            if final_metakernel_path.parent.is_dir() is False:
                logger.info(
                    f"Creating output directory for metakernel at {final_metakernel_path.parent}"
                )
                final_metakernel_path.parent.mkdir(parents=True, exist_ok=True)

            # copy metakernal_file_path to final output path
            logger.info(
                f"Copying metakernel from work folder to output path {final_metakernel_path}"
            )
            with open(metakernel_file_path) as src_file:
                with open(final_metakernel_path, "w") as dest_file:
                    dest_file.write(src_file.read())
        else:
            final_metakernel_path = metakernel_file_path

        if publish_to_datastore:
            final_metakernel_path = publish_spice_kernel(
                app_settings,
                metakernel_file_path,
                use_database=True,
            )

        logger.info(f"Generated SPICE metakernel at {final_metakernel_path}")

        return final_metakernel_path


def publish_spice_kernel(
    app_settings: AppSettings,
    kernal_file_path: Path,
    use_database: bool,
) -> Path:
    """Publish SPICE kernel to data store."""
    output_manager: IDatastoreFileManager = DatastoreFileManager.CreateByMode(
        app_settings,
        use_database=use_database,
    )

    handler = SPICEPathHandler.from_filename(kernal_file_path)

    if handler is None:
        raise RuntimeError(
            f"Downloaded SPICE file {kernal_file_path} could not be parsed into SPICEPathHandler"
        )

    (output_file, _) = output_manager.add_file(kernal_file_path, handler)

    return output_file


def _metakernel_builder(
    start_time: datetime | None,
    end_time: datetime | None,
    files: list[File],
    spice_folder: Path,
    file_types: set[str] | None = None,
) -> MetaKernel:
    """Create a MetaKernel class and inserts files into it."""

    # for all files with version > 1 we need to remove older versions
    files_grouped_by_path: dict[str, list] = {}
    for file in files:
        if file.path not in files_grouped_by_path:
            files_grouped_by_path[file.path] = []
        files_grouped_by_path[file.path].append(file)

    latest_files: list[File] = []
    for file_list in files_grouped_by_path.values():
        # sort by version descending
        # remove files missing the file_meta or version field as we cannot determine which is latest, and log a warning about this
        file_list_with_version = [
            f for f in file_list if f.file_meta and "version" in f.file_meta
        ]
        if len(file_list) != len(file_list_with_version):
            logger.warning(
                f"Found {len(file_list) - len(file_list_with_version)} files without version information for {file_list[0].path}. These files will be skipped as we cannot determine which is the latest version."
            )
        file_list_with_version.sort(
            key=lambda f: int(f.file_meta.get("version", "1")), reverse=True
        )
        latest_files.append(file_list_with_version[0])  # take the latest version only

    # get the first leapseconds kernel if available
    leapseconds_kernels = [
        f
        for f in latest_files
        if f.file_meta
        and f.file_meta.get(SPICE_FILE_META_FIELD_KERNAL_TYPE) == "leapseconds"
    ]
    if leapseconds_kernels:
        leap_file = spice_folder / leapseconds_kernels[0].path
        spiceypy.furnsh(str(leap_file))

    # filter files by start_time and end_time if provided
    if start_time and end_time:
        latest_files = [
            f
            for f in latest_files
            if (
                f.file_meta is None
                or f.file_meta.get("max_date_datetime") is None
                or (
                    TimeConversion.try_extract_iso_like_datetime(
                        f.file_meta, "min_date_datetime"
                    )
                    <= end_time
                    and TimeConversion.try_extract_iso_like_datetime(
                        f.file_meta, "max_date_datetime"
                    )
                    >= start_time
                )
            )
        ]

    latest_files.sort(
        key=lambda f: TimeConversion.try_extract_iso_like_datetime(
            f.metadata, SPICE_FILE_META_FIELD_INGESTION_DATE
        )
        or f.last_modified_date
    )
    if not latest_files:
        raise RuntimeError(
            "No SPICE files found in the database matching the time range."
        )

    logger.info(f"Generating SPICE metakernel with {len(latest_files)} files.")
    logger.info(json.dumps([f.path for f in files], indent=2))

    if not start_time:
        start_time = minimum_mission_time

    if not end_time:
        # fint the max end time of the files but only for attitute history kernels
        # use those because they are the crucial kernel that changes daily
        end_times = [
            TimeConversion.try_extract_iso_like_datetime(
                f.file_meta, "max_date_datetime"
            )
            for f in latest_files
            if f.file_meta
            and f.file_meta.get("max_date_datetime") is not None
            and f.file_meta.get(SPICE_FILE_META_FIELD_KERNAL_TYPE) == "attitude_history"
        ]
        if end_times:
            end_time = max([t for t in end_times if t is not None])
        else:
            end_time = DatetimeProvider.today()

    # Create the Metakernel class
    allowed_types = KernelCollection().category_types
    logger.debug(
        f"Metakernel start time: {start_time}, end time: {end_time}, allowed spice types: {allowed_types}"
    )
    latest_date = max(
        [
            TimeConversion.try_extract_iso_like_datetime(
                f.metadata, SPICE_FILE_META_FIELD_INGESTION_DATE
            )
            or f.last_modified_date
            for f in latest_files
        ]
    )

    metakernel = MetaKernel(
        int(TimeConversion.datetime_to_j2000(start_time)),
        int(TimeConversion.datetime_to_j2000(end_time)),
        allowed_spice_types=allowed_types,
        latest_file_date=latest_date,
    )

    for spice_category in KernelCollection().imap_spice_load_order:
        for spice_subtype in spice_category:
            if file_types and spice_subtype.name not in file_types:
                continue  # Skip over the file if not in requested list
            metadata_of_spice_file_of_selected_type = [
                {
                    "file_name": f.path,
                    "file_intervals_j2000": f.file_meta["file_intervals_j2000"],
                    "timestamp": f.file_meta["timestamp"],
                }
                for f in files
                if f.file_meta
                and (
                    f.file_meta.get(SPICE_FILE_META_FIELD_KERNAL_TYPE)
                    == spice_subtype.name.lower()
                )
            ]

            logger.debug(
                f"Loading {len(metadata_of_spice_file_of_selected_type)} files of type {spice_subtype.name} into metakernel."
            )
            metakernel.load_spice(
                metadata_of_spice_file_of_selected_type,
                spice_category.spice_category_name(),
                "file_intervals_j2000",
                priority_field="timestamp",
            )

    logger.info(f"Metakernel generated with {len(metakernel.spice_files)} SPICE files.")

    return metakernel
