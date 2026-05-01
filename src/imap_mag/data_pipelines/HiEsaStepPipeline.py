from imap_mag.client.WebTCADLaTiS import HKWebTCADItems, WebTCADLaTiS
from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import (
    AutomaticRunParameters,
    FetchByDatesRunParameters,
    Pipeline,
)
from imap_mag.data_pipelines.DownloadWebTCADCsvFilesStage import (
    DownloadWebTCADCsvFilesStage,
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


class HiEsaStepPipeline(Pipeline):
    """Pipeline that downloads daily IMAP-Hi ESA STEP telemetry CSV files from the WebTCAD LaTiS API.

    The same pipeline shape is used for both Hi-45 and Hi-90; the specific telemetry item
    (and therefore the TMID, instrument and descriptor) is provided at construction time.
    """

    def __init__(
        self,
        item: HKWebTCADItems,
        database: Database | None,
        settings: AppSettings,
    ):
        super().__init__(settings=settings)

        self.item = item
        self.progress_item_id = f"{item.instrument.short_name.upper()}_ESA_STEP"
        self.initial_context = {"progress_item_name": self.progress_item_id}
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
                DownloadWebTCADCsvFilesStage(
                    item=self.item,
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
