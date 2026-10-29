import fnmatch
from datetime import UTC, datetime

from imap_db.model import File, WorkflowProgress
from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import PROGRESS_DATE_CONTEXT_KEY, SourceStage
from imap_mag.data_pipelines.Record import Record
from imap_mag.data_pipelines.RunParameters import (
    AutomaticRunParameters,
    IndexByIdsRunParameters,
    IndexByPathOrModifiedDateRunParameters,
)
from imap_mag.db import Database


class GetFilesToIndexStage(SourceStage):
    """Source stage that retrieves files to index from the database.

    Behaviour depends on the run parameters passed to the pipeline:
    - AutomaticRunParameters: files modified since last workflow progress
    - IndexByIdsRunParameters: explicit file IDs
    - IndexByPathOrModifiedDateRunParameters: files matching path patterns,
      files modified within a date range, or both combined

    Whatever the mode, the ``paths_to_match`` filter and the ``max_files_per_run``
    limit given at construction are applied.  Deleted files are always skipped.

    Args:
        database: Database to query for files.
        settings: App settings, used to locate the data store.
        paths_to_match: fnmatch patterns a file path must match to be published.
            No filtering if empty.
        max_files_per_run: Maximum number of files to publish in one run.
            No limit if None.
    """

    def __init__(
        self,
        database: Database | None,
        settings: AppSettings,
        paths_to_match: list[str] | None = None,
        max_files_per_run: int | None = None,
    ):
        super().__init__()
        self.database = database
        self.settings = settings
        self.paths_to_match = paths_to_match or []
        self.max_files_per_run = max_files_per_run

    async def start(self, context: dict, **kwargs):
        files: list[File] = []

        run_params = self._run_parameters

        progress_item_name = context.get("progress_item_name")
        if progress_item_name is None:
            raise ValueError(
                "progress_item_name must be provided in context for GetFilesToIndexStage"
            )

        workflow_progress = (
            self.database.get_workflow_progress(progress_item_name)
            if self.database
            else WorkflowProgress(item_name=progress_item_name)
        )
        context["workflow_progress"] = workflow_progress

        if isinstance(run_params, IndexByIdsRunParameters):
            files = await self._get_files_by_ids(run_params.file_ids)
        elif isinstance(run_params, IndexByPathOrModifiedDateRunParameters):
            files = await self._get_files_by_path_or_modified_date(
                run_params.file_paths,
                run_params.modified_after,
                run_params.modified_before,
            )
        else:
            # AutomaticRunParameters: use workflow progress
            assert isinstance(run_params, AutomaticRunParameters)
            files = await self._get_files_by_path_or_modified_date(
                [], self._get_progress_timestamp(context), None
            )

        files = self._apply_max_files_limit(self._apply_paths_filter(files))

        if not files:
            self.logger.info("No files to index")
            return

        self.logger.info(f"Indexing {len(files)} files")

        for file in files:
            file_path = self.settings.data_store / file.path

            if not file_path.exists():
                self.logger.warning(
                    f"File {file.path} does not exist on disk, skipping"
                )
                continue

            # Keep PROGRESS_DATE_CONTEXT_KEY at the latest file modification date seen
            if file.last_modified_date:
                existing = context.get(PROGRESS_DATE_CONTEXT_KEY)
                if existing is None or file.last_modified_date > existing:
                    context[PROGRESS_DATE_CONTEXT_KEY] = file.last_modified_date

            await self.publish_next(
                Record(
                    file_id=file.id,
                    file_path=file_path,
                    file_path_relative=file.path,
                    last_modified_date=file.last_modified_date,
                ),
                context=context,
            )

    async def _get_files_by_ids(self, file_ids: list[int]) -> list[File]:
        if not file_ids:
            return []
        self.logger.info(f"Getting {len(file_ids)} files by ID: {file_ids}")
        if self.database is None:
            self.logger.warning("No database provided, cannot get files by ID")
            return []
        files = self.database.get_files_by_ids(file_ids)
        files = [f for f in files if f.deletion_date is None]
        self.logger.info(f"Found {len(files)} non-deleted files by ID")
        return files

    async def _get_files_by_path_or_modified_date(
        self,
        file_paths: list[str],
        modified_after: datetime | None,
        modified_before: datetime | None,
    ) -> list[File]:
        if self.database is None:
            self.logger.warning(
                "No database provided to GetFilesToIndexStage, nothing to do"
            )
            return []

        self.logger.info(
            f"Getting files matching paths {file_paths}, modified after {modified_after} and before {modified_before}"
        )
        files = self.database.get_active_files_by_path_or_modified_date(
            file_paths, modified_after, modified_before
        )
        self.logger.info(f"Found {len(files)} files")
        return files

    @staticmethod
    def _get_progress_timestamp(context: dict) -> datetime:
        """Get the time of the last progress, or the start of the mission if none."""
        workflow_progress = context.get("workflow_progress")
        if workflow_progress and workflow_progress.progress_timestamp:
            return workflow_progress.progress_timestamp
        return datetime(2010, 1, 1, tzinfo=UTC)

    def _apply_max_files_limit(self, files: list[File]) -> list[File]:
        """Limit the number of files analysed in one run.

        Files are expected in ascending last modified order. Files sharing the
        last modified date of the final file in the batch are kept together, so
        progress (which only moves forward past that date) does not skip any.
        """
        max_files = self.max_files_per_run
        if max_files is None or len(files) <= max_files:
            return files

        cutoff_date = files[max_files - 1].last_modified_date
        limited = files[:max_files]
        for file in files[max_files:]:
            if file.last_modified_date != cutoff_date:
                break
            limited.append(file)

        self.logger.info(
            f"Limiting run to {len(limited)}/{len(files)} files (max_files_per_run={max_files})"
        )
        return limited

    def _apply_paths_filter(self, files: list[File]) -> list[File]:
        paths_to_match = self.paths_to_match
        if not paths_to_match:
            return files
        filtered = [
            f for f in files if any(fnmatch.fnmatch(f.path, p) for p in paths_to_match)
        ]
        self.logger.info(
            f"After filtering with {len(paths_to_match)} patterns: {len(filtered)}/{len(files)} files match"
        )
        return filtered
