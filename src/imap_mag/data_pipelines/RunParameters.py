from dataclasses import dataclass
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
