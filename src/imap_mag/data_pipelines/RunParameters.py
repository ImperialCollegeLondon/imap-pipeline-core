from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Annotated

from pydantic import Field


class ProgressUpdateMode(Enum):
    AUTO_UPDATE_PROGRESS_IF_NEWER = "auto"
    NEVER_UPDATE_PROGRESS = "never"
    FORCE_UPDATE_PROGRESS = "force"


@dataclass
class PipelineRunParameters:
    progress_mode: Annotated[
        ProgressUpdateMode,
        Field(
            json_schema_extra={
                "title": "Progress save mode",
                "description": "Mode for updating workflow progress. 'auto' will update progress only if the new progress date is newer than the existing progress date. 'never' will not update progress. 'force' will update progress regardless of the existing progress date.",
            }
        ),
    ] = ProgressUpdateMode.AUTO_UPDATE_PROGRESS_IF_NEWER


@dataclass
class AutomaticRunParameters(PipelineRunParameters):
    ### Get only new data based on tracked progress from the last run
    pass


@dataclass
class FetchByDatesRunParameters(PipelineRunParameters):
    ### Get data between dates specified by the user but do not redownload data that has already been crawled for those dates
    start_date: Annotated[
        datetime | None,
        Field(
            json_schema_extra={
                "title": "Start date",
                "description": "Start date for the download. Default is after the last progress date.",
            }
        ),
    ] = None

    end_date: Annotated[
        datetime | None,
        Field(
            json_schema_extra={
                "title": "End date",
                "description": "End date for the download. Default is end of today.",
            }
        ),
    ] = None

    force_redownload: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Force redownload",
                "description": "If True, redownload data for all days in the date range even if previously crawled",
            }
        ),
    ] = False


@dataclass
class IndexByIdsRunParameters(PipelineRunParameters):
    """Manually index specific files by their database IDs.

    Defaults to NEVER_UPDATE_PROGRESS so scheduled automatic runs are unaffected.
    """

    progress_mode: ProgressUpdateMode = ProgressUpdateMode.NEVER_UPDATE_PROGRESS

    file_ids: Annotated[
        list[int],
        Field(
            json_schema_extra={
                "title": "File IDs",
                "description": "List of file IDs to index.",
            }
        ),
    ] = field(default_factory=list)


@dataclass
class IndexByDateRangeRunParameters(PipelineRunParameters):
    """Manually index files modified within a date range.

    Defaults to NEVER_UPDATE_PROGRESS so scheduled automatic runs are unaffected.
    """

    progress_mode: ProgressUpdateMode = ProgressUpdateMode.NEVER_UPDATE_PROGRESS

    modified_after: Annotated[
        datetime | None,
        Field(
            json_schema_extra={
                "title": "Modified after",
                "description": "Index files modified after this datetime.",
            }
        ),
    ] = None

    modified_before: Annotated[
        datetime | None,
        Field(
            json_schema_extra={
                "title": "Modified before",
                "description": "Index files modified before this datetime.",
            }
        ),
    ] = None


@dataclass
class IndexByFileNamesRunParameters(PipelineRunParameters):
    """Manually index files matching specific path patterns or exact paths.

    Defaults to NEVER_UPDATE_PROGRESS so scheduled automatic runs are unaffected.
    """

    progress_mode: ProgressUpdateMode = ProgressUpdateMode.NEVER_UPDATE_PROGRESS

    file_paths: Annotated[
        list[str],
        Field(
            json_schema_extra={
                "title": "File paths",
                "description": "List of file paths or fnmatch patterns to index.",
            }
        ),
    ] = field(default_factory=list)
