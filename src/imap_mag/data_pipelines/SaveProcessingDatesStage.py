from datetime import datetime

from imap_db.model import WorkflowProgress
from imap_mag.data_pipelines import (
    PROGRESS_DATE_CONTEXT_KEY,
    Pipeline,
    ProgressUpdateMode,
    Stage,
)
from imap_mag.data_pipelines.Record import Record
from imap_mag.db import Database


class SaveProcessingDatesStage(Stage):
    def __init__(
        self,
        database: Database | None,
    ):
        super().__init__()
        self.database = database
        self.have_saved_at_least_once = False

        if not self.database:
            self.logger.warning(
                "No database provided to SaveProcessingDatesStage, progress will not be saved!"
            )

    async def process(self, item: Record, context: dict, **kwargs):
        # progres timestamp could come from more than one place:
        # - try context first
        # - then the content dat of the file
        # TODO: Need to get the latest record date from the last file

        progress_date_context: datetime | None = (
            context.get(PROGRESS_DATE_CONTEXT_KEY) if hasattr(context, "get") else None
        )
        progres_date_file_record = (
            item.content_date if hasattr(item, "content_date") else None
        )

        progress_date = progress_date_context or progres_date_file_record

        self.update_workflow_progress(context, progress_date)

        # propagate item to next stage if needed
        await self.publish_next(item, context, **kwargs)

    def update_workflow_progress(self, context, progress_date):
        workflow_progress: WorkflowProgress = context["workflow_progress"]
        workflow_started = context.get(Pipeline.STARTED_CONTEXT_KEY)
        assert workflow_started is not None, "Pipeline start time must be in context"
        assert self._run_parameters is not None, (
            "Pipeline run parameters must be set before processing stages"
        )

        if (
            self.database
            and self._run_parameters.progress_mode
            != ProgressUpdateMode.NEVER_UPDATE_PROGRESS
        ):
            # current progress is later than the one on the database?
            if progress_date and (
                (workflow_progress.progress_timestamp is None)
                or (progress_date > workflow_progress.progress_timestamp)
                or self._run_parameters.progress_mode
                == ProgressUpdateMode.FORCE_UPDATE_PROGRESS
            ):
                workflow_progress.update_progress_timestamp(progress_date)

            workflow_progress.update_last_checked_timestamp(workflow_started)

            self.database.save(workflow_progress)
            self.have_saved_at_least_once = True

    async def stage_completed(self, context: dict):
        if not self.have_saved_at_least_once:
            self.update_workflow_progress(context, progress_date=None)

        return await super().stage_completed(context)
