import logging
from datetime import datetime
from typing import Annotated

import typer

from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.config import AppSettings
from imap_mag.db import Database
from imap_mag.db.HeliopuddleSync import run_sync

logger = logging.getLogger(__name__)


# E.g., imap-mag heliopuddle sync --full
def sync(
    full: Annotated[
        bool,
        typer.Option(
            help="Compare every file in the database with heliopuddle (the one-off migration). "
            "Otherwise only files modified since the last sync are synced.",
        ),
    ] = False,
    since: Annotated[
        datetime | None,
        typer.Option(help="Sync files modified after this time (UTC)."),
    ] = None,
    remove_untracked: Annotated[
        bool,
        typer.Option(
            help="With --full, also remove files that are in heliopuddle but not in the database.",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(help="Report what would change without changing heliopuddle."),
    ] = False,
) -> None:
    """Bring the heliopuddle project in line with the files table in the database."""

    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.heliopuddle)
    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    if not app_settings.heliopuddle.enabled:
        logger.error("heliopuddle integration is disabled in the configuration.")
        raise typer.Exit(code=1)

    summary = run_sync(
        app_settings,
        Database(),
        full=full,
        since=since,
        remove_untracked=remove_untracked,
        dry_run=dry_run,
    )
    prefix = "[DRY RUN] " if dry_run else ""
    logger.info(f"{prefix}heliopuddle sync complete: {summary.describe()}.")


app = typer.Typer()
app.command()(sync)
