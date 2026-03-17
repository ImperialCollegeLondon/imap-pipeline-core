import fnmatch
from datetime import UTC, datetime

from imap_db.model import File
from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import SourceStage
from imap_mag.data_pipelines.Record import Record
from imap_mag.data_pipelines.RunParameters import (
    AutomaticRunParameters,
    IndexByDateRangeRunParameters,
    IndexByFileNamesRunParameters,
    IndexByIdsRunParameters,
)
from imap_mag.db import Database


class GetFilesToIndexStage(SourceStage):
    """Source stage that retrieves files to index from the database.

    Behaviour depends on the run parameters passed to the pipeline:
    - AutomaticRunParameters: files modified since last workflow progress
    - IndexByIdsRunParameters: explicit file IDs
    - IndexByDateRangeRunParameters: files modified within a date range
    - IndexByFileNamesRunParameters: files matching path patterns

    Automatic and date-range modes also apply the paths_to_match filter from
    app settings.  Deleted files are always skipped.
    """

    def __init__(self, database: Database | None, settings: AppSettings):
        super().__init__()
        self.database = database
        self.settings = settings

    async def start(self, context: dict, **kwargs):
        files: list[File] = []

        run_params = self._run_parameters

        if isinstance(run_params, IndexByIdsRunParameters):
            files = await self._get_files_by_ids(run_params.file_ids)
        elif isinstance(run_params, IndexByFileNamesRunParameters):
            files = await self._get_files_by_paths(run_params.file_paths)
        elif isinstance(run_params, IndexByDateRangeRunParameters):
            files = await self._get_files_by_date_range(
                run_params.modified_after, run_params.modified_before, context
            )
        else:
            # AutomaticRunParameters: use workflow progress
            assert isinstance(run_params, AutomaticRunParameters)
            files = await self._get_files_automatic(context)

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

            await self.publish_next(
                Record(
                    file_id=file.id,
                    file_path=file_path,
                    file_path_relative=file.path,
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

    async def _get_files_by_paths(self, file_paths: list[str]) -> list[File]:
        if not file_paths:
            return []
        self.logger.info(f"Getting files by paths/patterns: {file_paths}")
        if self.database is None:
            self.logger.warning("No database provided, cannot get files by path")
            return []
        files = self.database.get_active_files_matching_patterns(file_paths)
        self.logger.info(f"Found {len(files)} files matching paths")
        return files

    async def _get_files_by_date_range(
        self,
        modified_after: datetime | None,
        modified_before: datetime | None,
        context: dict,
    ) -> list[File]:
        if self.database is None:
            self.logger.warning(
                "No database provided to GetFilesToIndexStage, nothing to do"
            )
            return []

        after = modified_after or datetime(2010, 1, 1, tzinfo=UTC)
        self.logger.info(f"Getting files modified after {after}")
        all_files = self.database.get_files_since(after)

        if modified_before is not None:
            all_files = [
                f
                for f in all_files
                if f.last_modified_date
                and f.last_modified_date.replace(tzinfo=UTC) <= modified_before
            ]

        return self._apply_paths_filter(all_files)

    async def _get_files_automatic(self, context: dict) -> list[File]:
        if self.database is None:
            self.logger.warning(
                "No database provided to GetFilesToIndexStage, nothing to do"
            )
            return []

        progress_item_name = context.get("progress_item_name")
        last_modified_date: datetime | None = None

        if progress_item_name:
            workflow_progress = self.database.get_workflow_progress(progress_item_name)
            context["workflow_progress"] = workflow_progress
            last_modified_date = workflow_progress.progress_timestamp

        if last_modified_date is None:
            last_modified_date = datetime(2010, 1, 1, tzinfo=UTC)

        self.logger.info(f"Getting files modified after {last_modified_date}")
        all_files = self.database.get_files_since(last_modified_date)
        self.logger.info(
            f"Found {len(all_files)} files modified after {last_modified_date}"
        )

        return self._apply_paths_filter(all_files)

    def _apply_paths_filter(self, files: list[File]) -> list[File]:
        paths_to_match = self.settings.file_index.paths_to_match
        if not paths_to_match:
            return files
        filtered = [
            f for f in files if any(fnmatch.fnmatch(f.path, p) for p in paths_to_match)
        ]
        self.logger.info(
            f"After filtering with {len(paths_to_match)} patterns: {len(filtered)}/{len(files)} files match"
        )
        return filtered
