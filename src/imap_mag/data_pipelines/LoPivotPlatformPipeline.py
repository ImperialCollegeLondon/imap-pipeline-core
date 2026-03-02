from imap_mag.client.WebTCADLaTiS import WebTCADLaTiS
from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import (
    AutomaticRunParameters,
    FetchByDatesRunParameters,
    Pipeline,
)
from imap_mag.data_pipelines.DownloadLoPivotCsvFilesStage import (
    DownloadLoPivotCsvFilesStage,
)
from imap_mag.data_pipelines.GetProcessingDatesStage import (
    DateResolutionMode,
    GetProcessingDatesStage,
)
from imap_mag.data_pipelines.PublishFileToDatastoreStage import (
    PublishFileToDatastoreStage,
)
from imap_mag.data_pipelines.SaveProcessingDatesStage import SaveProcessingDatesStage
from imap_mag.db import Database


class LoPivotPlatformPipeline(Pipeline):
    # static const:
    PROGRESS_ITEM_ID = "LO_PIVOT_PLATFORM_ANGLE"

    def __init__(
        self,
        database: Database | None,
        settings: AppSettings,
    ):
        super().__init__(settings=settings)

        self.initial_context = {"progress_item_name": self.PROGRESS_ITEM_ID}
        self._database = database
        self._client = WebTCADLaTiS(fetch_webtcad_config=settings.fetch_webtcad)

    def build(self, run_params: AutomaticRunParameters | FetchByDatesRunParameters):
        super().build(
            run_parameters=run_params,
            stages=[
                GetProcessingDatesStage(
                    database=self._database,
                    date_resolution_mode=DateResolutionMode.DATE_ONLY,
                ),
                DownloadLoPivotCsvFilesStage(
                    client=self._client,
                    settings=self._settings,
                ),
                PublishFileToDatastoreStage(
                    enabled=self._settings.fetch_webtcad.publish_to_data_store,
                    database=self._database,
                    settings=self._settings,
                ),
                SaveProcessingDatesStage(database=self._database),
            ],
        )
