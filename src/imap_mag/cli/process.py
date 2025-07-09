import logging
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appUtils
from imap_mag.cli.cliUtils import (
    fetch_file_for_work,
    initialiseLoggingForCommand,
)
from imap_mag.config import AppSettings, SaveMode
from imap_mag.io import (
    FilePathHandlerSelector,
    IFilePathHandler,
    InputManager,
)
from imap_mag.process import FileProcessor, dispatch

logger = logging.getLogger(__name__)


# E.g., imap-mag process solo_L2_mag-rtn-ll-internal_20240210_V00.cdf --save-mode localanddatabase
def process(
    files: Annotated[
        list[Path],
        typer.Argument(
            help="The file names or patterns to match for the input files",
            exists=False,  # can be a pattern
            file_okay=True,
            dir_okay=False,
            readable=True,
            writable=False,
        ),
    ],
    save_mode: Annotated[
        SaveMode,
        typer.Option(help="Whether to save locally only or to also save to database"),
    ] = SaveMode.LocalOnly,
) -> list[tuple[Path, IFilePathHandler]]:
    """Process a single file."""

    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.process)
    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    logger.info(f"Processing {len(files)} files:\n{', '.join(str(f) for f in files)}")

    input_manager = InputManager(app_settings.data_store)
    work_files: list[Path] = []

    for file in files:
        # If the file is not a relative/absolute path, try to find it in the datastore.
        if not file.exists():
            metadata_provider: IFilePathHandler | None = (
                FilePathHandlerSelector.find_by_path(file, throw_if_none_found=False)
            )

            if metadata_provider is not None:
                file = input_manager.get_versioned_file(
                    metadata_provider, latest_version=False, throw_if_none_found=True
                )
                assert file is not None

        matching_file: Path | None = fetch_file_for_work(
            file, work_folder, throw_if_not_found=True
        )
        assert matching_file is not None

        work_files.append(matching_file)

    # Process files.
    file_processor: FileProcessor = dispatch(work_files, work_folder, input_manager)
    file_processor.initialize(app_settings.packet_definition)

    processed_files: dict[Path, IFilePathHandler] = file_processor.process(work_files)

    # Copy files to the output directory.
    copied_files: list[tuple[Path, IFilePathHandler]] = []

    output_manager = appUtils.getOutputManagerByMode(
        app_settings.data_store,
        use_database=(save_mode == SaveMode.LocalAndDatabase),
    )

    for processed_file, path_handler in processed_files.items():
        (copied_file, path_handler) = output_manager.add_file(
            processed_file, path_handler
        )

        copied_files.append((copied_file, path_handler))

    return copied_files
