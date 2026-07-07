import logging
import os
from pathlib import Path
from typing import cast

from pydantic import BaseModel

from imap_mag.util.diskSpace import check_disk_space

logger = logging.getLogger(__name__)


class CommandConfig(BaseModel):
    _commmand_work_folder: Path | None = None

    work_sub_folder: str | None = None

    def setup_work_folder(
        self, app_settings, name_context: dict[str, str] | None = None
    ) -> Path:
        if self._commmand_work_folder is not None:
            return self._commmand_work_folder

        self._commmand_work_folder = cast(Path, app_settings.work_folder)

        sub_folder = self.work_sub_folder
        if sub_folder and name_context:
            # Allow placeholders like "calibrate_{date}_{mode}" to be filled in
            # from the command arguments so each run gets a unique work folder.
            sub_folder = sub_folder.format(**name_context)

        if sub_folder:
            self._commmand_work_folder = self._commmand_work_folder / sub_folder

        check_disk_space(self._commmand_work_folder, app_settings.disk_usage_threshold)

        if not os.path.exists(self._commmand_work_folder):
            logger.debug(f"Creating work folder {self._commmand_work_folder}")
            os.makedirs(self._commmand_work_folder)

        return self._commmand_work_folder
