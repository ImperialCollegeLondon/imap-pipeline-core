import asyncio
import logging
from typing import Annotated

import typer

from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.config import AppSettings
from imap_mag.db.DeleteOldRows import delete_old_rows
from imap_mag.util import DatetimeProvider

logger = logging.getLogger(__name__)


# E.g., imap-mag delete delete_old_database_rows
def delete_old_database_rows(
    dry_run: Annotated[
        bool | None,
        typer.Option(
            help="If set, only report the number of rows that would be deleted, "
            "without deleting them. If omitted, uses the value from configuration.",
        ),
    ] = True,
) -> None:
    """Delete rows from a PostgreSQL database table that are older than a threshold."""

    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(
        app_settings.database_delete_rows
    )
    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    affected = asyncio.run(delete_old_rows(app_settings, dry_run, DatetimeProvider()))
    tables = [task.table for task in app_settings.database_delete_rows.tasks]

    action_word = "would be" if dry_run else "were"
    logger.info(
        f"{affected} total row(s) {action_word} deleted from tables: '{tables}'."
    )


app = typer.Typer()
app.command()(delete_old_database_rows)
