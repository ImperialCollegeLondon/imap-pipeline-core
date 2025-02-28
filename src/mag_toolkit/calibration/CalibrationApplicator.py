import logging
from pathlib import Path

import numpy as np

from ..CDFLoader import load_cdf, write_cdf
from .CalibrationExceptions import CalibrationValidityError
from .calibrationFormat import CalibrationFormat
from .calibrationFormatProcessor import (
    CalibrationFormatProcessor,
)

logger = logging.getLogger(__name__)


class CalibrationApplicator:
    def apply(self, calibrationFile, dataFile, outputFile) -> Path:
        """Currently operating on unprocessed data."""
        data = load_cdf(dataFile)
        calibrationCollection: CalibrationFormat = (
            CalibrationFormatProcessor.loadFromPath(calibrationFile)
        )

        logger.info("Loaded calibration file and data file")

        try:
            self.checkValidity(data, calibrationCollection)
        except CalibrationValidityError as e:
            logger.info(f"{e} -> continuing application of calibration regardless")

        logger.info("Dataset and calibration file deemed compatible")

        for eachCal in calibrationCollection.calibrations:
            data.vectors[0] = data.vectors[0] + eachCal.offsets.X
            data.vectors[1] = data.vectors[1] + eachCal.offsets.Y
            data.vectors[2] = data.vectors[2] + eachCal.offsets.Z

        write_cdf(data, outputFile)

        return outputFile

    def checkValidity(self, data, calibrationCollection):
        # check for time validity
        if data.epoch[0] < np.datetime64(
            calibrationCollection.valid_start
        ) or data.epoch[1] > np.datetime64(calibrationCollection.valid_end):
            logger.debug("Data outside of calibration validity range")
            raise CalibrationValidityError("Data outside of calibration validity range")
