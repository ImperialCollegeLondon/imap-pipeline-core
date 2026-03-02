from datetime import datetime, timedelta

from imap_mag.client.WebTCADLaTiS import HKWebTCADItems, WebTCADLaTiS
from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import PROGRESS_DATE_CONTEXT_KEY, FileRecord, Record, Stage
from imap_mag.io.file import HKDecodedPathHandler
from imap_mag.util.Subsystem import Subsystem


class DownloadLoPivotCsvFilesStage(Stage):
    def __init__(self, client: WebTCADLaTiS, settings: AppSettings):
        super().__init__()
        self.client = client
        self.settings = settings

    def prepare(self, run_parameters, next_stage, index):
        self.work_folder = self.settings.setup_work_folder_for_command(
            self.settings.fetch_webtcad
        )

        return super().prepare(run_parameters, next_stage, index)

    async def process(self, item: Record, context: dict, **kwargs):
        if not item or not item.start_date or not item.end_date:
            raise ValueError(
                "DownloadLoPivotCsvFilesStage requires a Record with start_date and end_date"
            )

        current_date: datetime = item.start_date  # type: ignore
        end_date: datetime = item.end_date  # type: ignore

        while current_date <= end_date:
            next_date = current_date + timedelta(days=1)

            self.logger.info(f"Downloading day {current_date.strftime('%Y-%m-%d')}...")

            csv_content = self.client.download_imap_lo_pivot_platform_angle_to_csv_file(
                start_date=current_date,
                end_date=next_date,
                system_id=self.settings.fetch_webtcad.api.system_id,
                mode=WebTCADLaTiS.TimeQueryMode.SPACECRAFT_TIME_MODE,
            )

            if not csv_content or csv_content.strip() == "":
                raise RuntimeError(
                    f"Received empty CSV content for date {current_date.strftime('%Y-%m-%d')}"
                )

            # Check if the CSV has actual data (more than just a header line)
            lines = csv_content.strip().splitlines()
            if len(lines) <= 1:
                self.logger.info(
                    f"No data for {current_date.strftime('%Y-%m-%d')}. Skipping."
                )
            else:
                # Write CSV to a temporary file in the work folder
                filename = HKDecodedPathHandler(
                    instrument=Subsystem.LO.short_name,
                    descriptor=HKWebTCADItems.LO_PIVOT_PLATFORM_ANGLE.descriptor,
                    content_date=current_date,
                    extension="csv",
                ).get_filename()
                temp_csv_path = self.work_folder / filename
                temp_csv_path.write_text(csv_content)

                context[PROGRESS_DATE_CONTEXT_KEY] = current_date

                await self.publish_next(
                    FileRecord(temp_csv_path, current_date), context, **kwargs
                )

            current_date = next_date
