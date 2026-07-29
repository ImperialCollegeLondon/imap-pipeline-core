from datetime import datetime

from pydantic import BaseModel

from imap_mag.util import ScienceMode
from mag_toolkit.calibration import Sensor


class CalibrationJobParameters(BaseModel):
    date: datetime
    mode: ScienceMode
    sensor: Sensor
    cleanup_temp_files_after_run: bool = True
