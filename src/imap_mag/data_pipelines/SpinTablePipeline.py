from imap_mag.client.SDCDataAccess import SDCDataAccess
from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import (
    AutomaticRunParameters,
    FetchByDatesRunParameters,
    Pipeline,
)
from imap_mag.data_pipelines.DownloadSpinTableFilesStage import (
    DownloadSpinTableFilesStage,
)
from imap_mag.data_pipelines.GetProcessingDatesStage import (
    DateResolutionMode,
    GetProcessingDatesStage,
)
from imap_mag.data_pipelines.SaveProcessingDatesStage import SaveProcessingDatesStage
from imap_mag.db import Database
from imap_mag.util.DatetimeProvider import DatetimeProvider


class SpinTablePipeline(Pipeline):
    PROGRESS_ITEM_ID = "SPIN_TABLE"

    def __init__(
        self,
        database: Database | None,
        settings: AppSettings,
        client: SDCDataAccess,
        datetime_provider: DatetimeProvider = DatetimeProvider(),
    ):
        super().__init__(settings=settings, datetime_provider=datetime_provider)

        self.initial_context = {"progress_item_name": self.PROGRESS_ITEM_ID}
        self._database = database
        self._client = client

    def build(self, run_params: AutomaticRunParameters | FetchByDatesRunParameters):
        super().build(
            run_parameters=run_params,
            stages=[
                GetProcessingDatesStage(
                    database=self._database,
                    date_resolution_mode=DateResolutionMode.DATE_ONLY,
                    datetime_provider=self._datetime_provider,
                ),
                DownloadSpinTableFilesStage(
                    client=self._client,
                    settings=self._settings,
                    database=self._database,
                ),
                SaveProcessingDatesStage(database=self._database),
            ],
        )
