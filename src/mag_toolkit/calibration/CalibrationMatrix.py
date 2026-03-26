from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
from cdflib.xarray import cdf_to_xarray, xarray_to_cdf
from imap_processing.mag.l2 import mag_l2, mag_l2_data

from mag_toolkit.calibration.CalibrationDefinitions import (
    CONSTANTS,
)
from mag_toolkit.calibration.CalibrationExceptions import CalibrationValidityError

import numpy.typing as npt
import xarray as xr
import logging

logger = logging.getLogger(__name__)



class CalibrationMatrix:

	@classmethod
    def get_combined_epoch_dataset_for_imap_processing(
        cls, mag_l2_dataset: xr.Dataset, start_date: datetime, end_date: datetime
    ) -> xr.Dataset:
		# Need to epochs to cal files so that mag_l2 cdf code in imap_processing knows to apply the cals
        
        output_dataset = xr.Dataset()
        epoch_data = xr.date_range(
            start_date, end_date, freq="D"
        ).values.astype("datetime64[D]")
        output_dataset = output_dataset.assign_coords({self.time_variable: epoch_data})

    @classmethod
    def get_rotation_dataset_by_cdf_file(cls, cdf_file_path: Path):

        if not cdf_file_path.exists():
            raise FileNotFoundError(
                f"Rotation/calibration matrix file {cdf_file_path!s} not found"
            )

        referenceDataset = cdf_to_xarray(
            cdf_file_path,
            to_datetime=False,
        )

        logger.info(
                f"Using rotation file {cdf_file_path!s} for calibration application."
            )

        return referenceDataset


    @classmethod
    def get_zero_rotation_dataset(cls):
        """
        gets a unity matrix for all ranges and both sensors - so no actual rotation/calibration

        equal to cdf_to_xarray(
            "folder/imap_mag_l2-calibration_yyyyMMdd_v000.cdf",
            to_datetime=False,
        )
        where it has the unity matrixes inside
        """

        # Global attributes
        gatt = {
            "Project": ["STP>Solar-Terrestrial Physics"],
            "Source_name": ["IMAP"],
            "Discipline": ["Space Physics>Heliospheric Physics"],
            "Data_type": ["L2-calibration>Level-2 calibration matrices"],
            "Descriptor": ["MAG>Magnetometer"],
            "Data_version": ["v004"],
            "Generation_date": ["20250327"],
            "Logical_file_id": ["imap_mag_l2-calibration_20251017"],
            "Logical_source": ["imap_mag_l2_calibration"],
            "Logical_source_description": ["Level 2 Calibration Data"],
            "Mission_group": ["IMAP"],
            "PI_affiliation": ["Imperial College London"],
            "TEXT": [
                "The IMAP magnetometer requires dynamic calibration to remove the magnetic field"
                " of the spacecraft. The matrices should be applied per sensor and per range to"
                " correct from the Uunit Reference Frame (URF) to the orthogonal reference frame (ORF)"
            ],
        }

        # Coordinate: range (uint8, values 0-3)
        range_coord = xr.Variable("range", np.array([0, 1, 2, 3], dtype=np.uint8))

        # EXAMPLE of a slight rotation:
        # # URFTOORFO: 4x3x3 float32 calibration matrices (outer-sensor URF to ORF)
        # urftoorfo_data = np.array(
        #     [
        #         [
        #             [1.001621, -0.024766, -0.051825],
        #             [0.0, 1.000979, 0.044276],
        #             [0.0, 0.0, 1.0],
        #         ],
        #         [
        #             [0.999963, -0.024759, -0.009875],
        #             [0.0166231, 1.000706, 0.044109],
        #             [0.0, 0.0, 0.9998],
        #         ],
        #         [
        #             [1.02786, 0.003252, -0.018275],
        #             [0.0, 0.987654, 0.003198],
        #             [0.0, 0.015278, 0.993245],
        #         ],
        #         [
        #             [0.998997, -0.016221, 0.007253],
        #             [0.010803, 1.026376, 0.087256],
        #             [-0.03247, 0.0, 1.000561],
        #         ],
        #     ],
        #     dtype=np.float32,
        # )

        # 4x3x3 float32 identity matrices (inner-sensor URF to ORF)
        urftoorfo_data = np.tile(np.eye(3, dtype=np.float32), (4, 1, 1))
        urftoorfi_data = np.tile(np.eye(3, dtype=np.float32), (4, 1, 1))

        created_coord_vars: dict[str, xr.Variable] = {"range": range_coord}

        created_data_vars: dict[str, xr.Variable] = {
            "URFTOORFO": xr.Variable(
                ("range", "dim0", "dim0"), urftoorfo_data, attrs={"DEPEND_0": "range"}
            ),
            "URFTOORFI": xr.Variable(
                ("range", "dim0", "dim0"), urftoorfi_data, attrs={"DEPEND_0": "range"}
            ),
        }

        dataset = xr.Dataset(
            data_vars=created_data_vars,
            coords=created_coord_vars,
            attrs=gatt,
        )

        logger.info(
                f"Using unity matrix (so zero rotation) for calibration application."
            )

        return dataset
