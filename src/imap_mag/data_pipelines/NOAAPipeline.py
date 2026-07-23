from imap_mag.client.NOAAApiClient import NOAARTSWApiClient
from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import (
    AutomaticRunParameters,
    FetchByDatesRunParameters,
    Pipeline,
)
from imap_mag.data_pipelines.DownloadNOAAStage import DownloadNOAAStage
from imap_mag.data_pipelines.GetProcessingDatesStage import (
    DateResolutionMode,
    GetProcessingDatesStage,
)
from imap_mag.data_pipelines.PublishFileToDatastoreStage import (
    PublishFileToDatastoreStage,
)
from imap_mag.data_pipelines.SaveProcessingDatesStage import SaveProcessingDatesStage
from imap_mag.db import Database
from imap_mag.download.FetchNOAA import FetchNOAA
from imap_mag.io import FileFinder
from imap_mag.util.DatetimeProvider import DatetimeProvider


class NOAAPipeline(Pipeline):
    def __init__(
        self,
        spacecraft: str,
        instrument: str,
        database: Database | None,
        settings: AppSettings,
        datetime_provider: DatetimeProvider = DatetimeProvider(),
    ):
        super().__init__(settings=settings, datetime_provider=datetime_provider)

        self.spacecraft = spacecraft
        self.instrument = instrument

        self.initial_context = {
            "progress_item_name": f"{spacecraft.upper()}_{instrument.upper()}",
        }

        self._database = database

        self._client = NOAARTSWApiClient(
            settings.fetch_solar1_ace.api.url_base,
        )

        datastore_finder = FileFinder(settings.data_store)
        work_folder = settings.setup_work_folder_for_command(settings.fetch_solar1_ace)

        self._fetcher = FetchNOAA(
            data_access=self._client,
            work_folder=work_folder,
            datastore_finder=datastore_finder,
        )

        self._datetime_provider = datetime_provider

    def build(self, run_params: AutomaticRunParameters | FetchByDatesRunParameters):  # type: ignore
        super().build(
            run_parameters=run_params,
            stages=[
                GetProcessingDatesStage(
                    database=self._database,
                    date_resolution_mode=DateResolutionMode.EXACT_DATETIME,
                    datetime_provider=self._datetime_provider,
                ),
                DownloadNOAAStage(
                    spacecraft=self.spacecraft,
                    instrument=self.instrument,
                    fetcher=self._fetcher,
                ),
                PublishFileToDatastoreStage(
                    enabled=self._settings.fetch_solar1_ace.publish_to_data_store,
                    database=self._database,
                    settings=self._settings,
                ),
                SaveProcessingDatesStage(database=self._database),
            ],
        )
