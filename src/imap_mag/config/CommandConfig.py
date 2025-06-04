import logging
import os
from pathlib import Path
from typing import cast

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class CommandConfig(BaseModel):
    _commmand_work_folder: Path | None = None

    work_sub_folder: str | None = None

    def setup_work_folder(self, app_settings) -> Path:
        if self._commmand_work_folder is not None:
            return self._commmand_work_folder

        self._commmand_work_folder = cast(Path, app_settings.work_folder)

        if self.work_sub_folder:
            self._commmand_work_folder = (
                self._commmand_work_folder / self.work_sub_folder
            )

        if not os.path.exists(self._commmand_work_folder):
            logger.info(f"Creating work folder {self._commmand_work_folder}")
            os.makedirs(self._commmand_work_folder)

        return self._commmand_work_folder
