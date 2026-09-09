import asyncio
import logging
from typing import Annotated

import typer

from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.config import AppSettings
from imap_mag.db.DeleteOldRows import delete_old_rows
from imap_mag.util import DatetimeProvider
from prefect_server.postgresUploadFlow import _get_database_connectionstring

logger = logging.getLogger(__name__)


# E.g., imap-mag delete-old-database-rows --table ace_mag_noaa --older-than-days 7
def delete_old_database_rows(
    table: Annotated[
        str,
        typer.Option(
            "--table",
            help="Name of the database table to delete rows from.",
        ),
    ],
    older_than_days: Annotated[
        int,
        typer.Option(
            "--older-than-days",
            help="Rows where datetime_column is older than this many days will be deleted.",
        ),
    ] = 7,
    datetime_column: Annotated[
        str,
        typer.Option(
            "--datetime-column",
            help="Name of the datetime column used to determine the age of a row.",
        ),
    ] = "id",
    dry_run: Annotated[
        bool | None,
        typer.Option(
            help="If set, only report the number of rows that would be deleted, "
            "without deleting them. If omitted, uses the value from configuration.",
        ),
    ] = None,
) -> None:
    """Delete rows from a PostgreSQL database table that are older than a threshold."""

    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(
        app_settings.delete_old_database_rows
    )
    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    config = app_settings.delete_old_database_rows
    dry_run = dry_run if dry_run is not None else config.dry_run

    db_url = asyncio.run(
        _get_database_connectionstring(
            app_settings, config.database_url_env_var_or_block_name
        )
    )

    logger.info(
        f"Deleting rows from '{table}' where {datetime_column} is older than "
        f"{older_than_days} day(s). Dry run: {dry_run}"
    )

    affected = delete_old_rows(
        db_url, table, datetime_column, older_than_days, dry_run, DatetimeProvider()
    )

    action_word = "would be" if dry_run else "were"
    logger.info(f"{affected} row(s) {action_word} deleted from '{table}'.")
