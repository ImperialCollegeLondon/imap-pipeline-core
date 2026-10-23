import logging

from imap_mag.data_pipelines.Pipeline import (
    Pipeline,
)
from imap_mag.data_pipelines.Record import FileRecord, Record
from imap_mag.data_pipelines.Result import Result
from imap_mag.data_pipelines.RunParameters import (
    AutomaticRunParameters,
    FetchByDatesRunParameters,
    IndexByDateRangeRunParameters,
    IndexByFileNamesRunParameters,
    IndexByIdsRunParameters,
    PipelineRunParameters,
    ProgressUpdateMode,
)
from imap_mag.data_pipelines.Stages import SourceStage, Stage

logging.getLogger("imap_mag.data_pipelines").setLevel(logging.INFO)

PROGRESS_DATE_CONTEXT_KEY = "progress_date"

__all__ = [
    "AutomaticRunParameters",
    "FetchByDatesRunParameters",
    "FileRecord",
    "IndexByDateRangeRunParameters",
    "IndexByFileNamesRunParameters",
    "IndexByIdsRunParameters",
    "Pipeline",
    "PipelineRunParameters",
    "ProgressUpdateMode",
    "Record",
    "Result",
    "SourceStage",
    "Stage",
]
