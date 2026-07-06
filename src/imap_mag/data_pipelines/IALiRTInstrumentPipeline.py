from imap_mag.client.IALiRTApiClient import IALiRTApiClient
from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import (
    AutomaticRunParameters,
    FetchByDatesRunParameters,
    Pipeline,
)
from imap_mag.data_pipelines.DownloadIALiRTStage import DownloadIALiRTStage
from imap_mag.data_pipelines.GetProcessingDatesStage import (
    DateResolutionMode,
    GetProcessingDatesStage,
)
from imap_mag.data_pipelines.PublishFileToDatastoreStage import (
    PublishFileToDatastoreStage,
)
from imap_mag.data_pipelines.SaveProcessingDatesStage import SaveProcessingDatesStage
from imap_mag.db import Database
from imap_mag.download.FetchIALiRT import FetchIALiRT
from imap_mag.io import FileFinder
from imap_mag.util.constants import CONSTANTS
from imap_mag.util.DatetimeProvider import DatetimeProvider


class IALiRTPipeline(Pipeline):
    def __init__(
        self,
        instrument: str,
        database: Database | None,
        settings: AppSettings,
        datetime_provider: DatetimeProvider = DatetimeProvider(),
    ):
        super().__init__(settings=settings, datetime_provider=datetime_provider)
        self.instrument = instrument

        self.is_hk = instrument.endswith("_hk")

        if self.is_hk:
            progress_id = f"{CONSTANTS.DATABASE.IALIRT_HK_PROGRESS_ID}"
        else:
            progress_id = (
                f"{self.instrument.upper()}_{CONSTANTS.DATABASE.IALIRT_PROGRESS_ID}"
            )
        self.initial_context = {
            "progress_item_name": progress_id,
        }

        self._database = database

        self._client = IALiRTApiClient(
            auth_code=settings.fetch_ialirt.api.auth_code,
            sdc_url=settings.fetch_ialirt.api.url_base,
        )

        datastore_finder = FileFinder(settings.data_store)
        work_folder = settings.setup_work_folder_for_command(settings.fetch_ialirt)

        self._fetcher = FetchIALiRT(
            data_access=self._client,
            work_folder=work_folder,
            datastore_finder=datastore_finder,
            packet_definition=settings.packet_definition,
        )

    def build(self, run_params: AutomaticRunParameters | FetchByDatesRunParameters):  # type: ignore
        super().build(
            run_parameters=run_params,
            stages=[
                GetProcessingDatesStage(
                    database=self._database,
                    date_resolution_mode=DateResolutionMode.DATE_ONLY,
                    datetime_provider=self._datetime_provider,
                ),
                DownloadIALiRTStage(
                    instrument=self.instrument,
                    fetcher=self._fetcher,
                ),
                PublishFileToDatastoreStage(
                    enabled=self._settings.fetch_ialirt.publish_to_data_store,
                    database=self._database,
                    settings=self._settings,
                ),
                SaveProcessingDatesStage(database=self._database),
            ],
        )
