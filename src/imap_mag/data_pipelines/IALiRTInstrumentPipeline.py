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


class IALiRTInstrumentPipeline(Pipeline):
    """Pipeline that downloads I-ALiRT data for a single instrument."""

    def __init__(
        self,
        instrument: str,
        database: Database | None,
        settings: AppSettings,
    ):
        super().__init__(settings=settings)

        if instrument not in CONSTANTS.DATABASE.IALIRT_INSTRUMENT_PROGRESS_IDS:
            raise ValueError(
                f"Unknown I-ALiRT instrument '{instrument}'. "
                f"Supported: {list(CONSTANTS.DATABASE.IALIRT_INSTRUMENT_PROGRESS_IDS.keys())}"
            )

        self._instrument = instrument
        self._database = database

        progress_item_name = CONSTANTS.DATABASE.IALIRT_INSTRUMENT_PROGRESS_IDS[
            instrument
        ]
        self.initial_context = {"progress_item_name": progress_item_name}

        data_access = IALiRTApiClient(
            settings.fetch_ialirt.api.auth_code,
            settings.fetch_ialirt.api.url_base,
        )
        work_folder = settings.setup_work_folder_for_command(settings.fetch_ialirt)
        datastore_finder = FileFinder(settings.data_store)

        self._fetcher = FetchIALiRT(
            data_access, work_folder, datastore_finder, settings.packet_definition
        )

    def build(self, run_params: AutomaticRunParameters | FetchByDatesRunParameters):
        super().build(
            run_parameters=run_params,
            stages=[
                GetProcessingDatesStage(
                    database=self._database,
                    date_resolution_mode=DateResolutionMode.EXACT_DATETIME,
                ),
                DownloadIALiRTStage(
                    instrument=self._instrument,
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
