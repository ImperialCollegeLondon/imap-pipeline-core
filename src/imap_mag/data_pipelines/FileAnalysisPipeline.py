from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import Pipeline, PipelineRunParameters
from imap_mag.data_pipelines.AnalyseFilesStage import AnalyseFilesStage
from imap_mag.data_pipelines.GetFilesToIndexStage import GetFilesToIndexStage
from imap_mag.data_pipelines.SaveDatabaseItemsStage import SaveDatabaseItemsStage
from imap_mag.data_pipelines.SaveProcessingDatesStage import SaveProcessingDatesStage
from imap_mag.db import Database


class FileAnalysisPipeline(Pipeline):
    """Pipeline that indexes metadata about data files into the file_analysis database table."""

    PROGRESS_ITEM_ID = "FILE_ANALYSER"

    def __init__(self, database: Database | None, settings: AppSettings):
        super().__init__(settings=settings)
        self.initial_context = {"progress_item_name": self.PROGRESS_ITEM_ID}
        self._database = database

    def build(self, run_parameters: PipelineRunParameters, **_kwargs):  # type: ignore[override]
        super().build(
            run_parameters=run_parameters,
            stages=[
                GetFilesToIndexStage(
                    database=self._database,
                    settings=self._settings,
                ),
                AnalyseFilesStage(settings=self._settings),
                SaveDatabaseItemsStage(database=self._database),
                SaveProcessingDatesStage(database=self._database),
            ],
        )
