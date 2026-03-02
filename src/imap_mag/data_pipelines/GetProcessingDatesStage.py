from enum import Enum

from imap_db.model import WorkflowProgress
from imap_mag.cli.fetch.DownloadDateManager import DownloadDateManager
from imap_mag.data_pipelines import (
    AutomaticRunParameters,
    FetchByDatesRunParameters,
    ProgressUpdateMode,
    SourceStage,
)
from imap_mag.data_pipelines.Record import Record
from imap_mag.db import Database
from imap_mag.util import DatetimeProvider


class DateResolutionMode(Enum):
    EXACT_DATETIME = 0
    DATE_ONLY = 1


class GetProcessingDatesStage(SourceStage):
    def __init__(
        self,
        database: Database | None,
        date_resolution_mode: DateResolutionMode = DateResolutionMode.EXACT_DATETIME,
    ):
        super().__init__()
        self.database = database
        self.mode = date_resolution_mode

    async def start(self, context: dict, **kwargs):
        requested_start_date = None
        requested_end_date = None
        force_redownload = False
        progress_item_name = context.get("progress_item_name")

        if progress_item_name is None:
            raise ValueError(
                "progress_item_name must be provided in context for GetProcessingDatesStage"
            )

        if isinstance(self._run_parameters, FetchByDatesRunParameters):
            requested_start_date = self._run_parameters.start_date
            requested_end_date = self._run_parameters.end_date
            force_redownload = self._run_parameters.force_redownload
        else:
            assert isinstance(self._run_parameters, AutomaticRunParameters)

        workflow_progress = (
            self.database.get_workflow_progress(progress_item_name)
            if self.database
            else WorkflowProgress(item_name=progress_item_name)
        )
        context["workflow_progress"] = workflow_progress

        date_manager = DownloadDateManager(progress_item_name, self.database)

        download_dates = date_manager.get_dates_for_download(
            original_start_date=requested_start_date,
            original_end_date=requested_end_date,
            validate_with_database=self.database is not None,
            workflow_progress=workflow_progress,
        )  # type: ignore

        if download_dates is None:
            (download_start, download_end) = (None, None)
        else:
            (download_start, download_end) = download_dates

        download_start = requested_start_date if force_redownload else download_start
        download_end = requested_end_date if force_redownload else download_end

        if download_end is None and download_start is not None:
            download_end = DatetimeProvider.end_of_today()

        # nothing to do? record we check it and exit
        if download_start is None and download_end is None:
            if (
                self._run_parameters.progress_mode
                != ProgressUpdateMode.NEVER_UPDATE_PROGRESS
            ):
                workflow_progress.update_last_checked_timestamp(
                    context.get("started", DatetimeProvider.now())
                )
            return

        if self.mode == DateResolutionMode.DATE_ONLY:
            download_start = download_start.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            download_end = download_end.replace(
                hour=23, minute=59, second=59, microsecond=999999
            )

        assert download_start and download_end, (
            "Download start and end must be defined at this point"
        )

        self.logger.info(
            f"Using {progress_item_name} dates {download_start} to {download_end}."
        )

        await self.publish_next(
            Record(start_date=download_start, end_date=download_end), context=context
        )
