import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import xarray as xr
from cdflib.xarray import cdf_to_xarray

logger = logging.getLogger(__name__)

epoch_dimension = "epoch"  # Same as imap_processing AncillaryCombiner.time_variable


class CalibrationMatrix:
    @classmethod
    def get_combined_epoch_dataset_for_imap_processing(
        cls,
        calibration_dataset: xr.Dataset,
        calibration_dataset_start_date: datetime,
        calibration_dataset_end_date: datetime,
        calibration_dataset_version: int = 0,
    ) -> xr.Dataset:
        # Need to epochs to cal files so that mag_l2 cdf code in imap_processing knows to apply the cals
        # this method re-implments the logic in imap_processing AncillaryCombiner._combine_input_datasets() for a single cal file dataset

        output_dataset = xr.Dataset()
        epoch_data = xr.date_range(
            calibration_dataset_start_date, calibration_dataset_end_date, freq="D"
        ).values.astype("datetime64[D]")
        output_dataset = output_dataset.assign_coords({epoch_dimension: epoch_data})

        for data_var in calibration_dataset.data_vars:
            shape = calibration_dataset[data_var].shape
            var_type = calibration_dataset[data_var].dtype
            if issubclass(var_type.type, np.integer):
                maxval = np.iinfo(var_type).max
            else:
                maxval = np.iinfo(np.int32).max
            output_dataset[data_var] = xr.DataArray(
                np.full((len(epoch_data), *shape), maxval, dtype=var_type),
                dims=[epoch_dimension]
                + [f"{data_var}_dim_{i}" for i in range(len(shape))],
            )

        output_dataset["input_file_version"] = xr.DataArray(
            np.zeros((len(epoch_data),)), dims=[epoch_dimension]
        )

        for date in xr.date_range(
            calibration_dataset_start_date, calibration_dataset_end_date, freq="D"
        ):
            np_date = np.datetime64(date, "D")
            for data_var in output_dataset.data_vars.keys():
                # find the index in output_dataset where date is equal to epoch
                index = output_dataset.get_index(epoch_dimension).get_loc(np_date)
                # For each data_var, fill the date in output_dataset with the
                # data_var from the input dataset.
                if str(data_var) in "input_file_version":
                    output_dataset["input_file_version"].data[index] = (
                        calibration_dataset_version
                    )
                else:
                    output_dataset[data_var].data[index] = calibration_dataset[
                        data_var
                    ].data

        return output_dataset

    @classmethod
    def get_rotation_dataset_by_cdf_file(cls, cdf_file_path: Path):
        if not cdf_file_path.exists():
            raise FileNotFoundError(
                f"Rotation/calibration matrix file {cdf_file_path!s} not found"
            )

        referenceDataset = cdf_to_xarray(
            str(cdf_file_path),
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
            "Data_version": ["v000"],
            "Generation_date": ["20250101"],
            "Logical_file_id": ["imap_mag_l2-calibration_20250101"],
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

        sensor_coord = xr.Variable("sensor", np.array(["MAGo", "MAGi"], dtype="<U4"))
        range_coord = xr.Variable("range", np.array([0, 1, 2, 3], dtype=np.int8))
        axis_coord = xr.Variable("axis", np.array(["x", "y", "z"], dtype="<U1"))

        # offsets_data = np.array(
        #     [
        #         [  # MAGO (range 0-3)
        #             [0, 0, 0],
        #             [0, 0, 0],
        #             [0, 0, 0],
        #             [0, 0, 0],
        #         ],
        #         [  # MAGI (range 0-3)
        #             [0, 0, 0],
        #             [0, 0, 0],
        #             [0, 0, 0],
        #             [0, 0, 0],
        #         ],
        #     ],
        #     dtype=np.float64,
        # )

        created_coord_vars: dict[str, xr.Variable] = {
            "sensor": sensor_coord,
            "range": range_coord,
            "axis": axis_coord,
        }

        i = np.eye(3, 3)
        urftoorfo_data = np.stack([i, i, i, i], axis=2)
        urftoorfi_data = np.stack([i, i, i, i], axis=2)

        valid_start_datetime = (
            (datetime(2025, 9, 26) - datetime(2000, 1, 1, 12, 0)).total_seconds() * 1e9
        )  # in nanoseconds since 2000-01-01T12:00:00, which is the CDF epoch time used in the files

        # Commented out vars are L1D cal options that are not used here but left for reference
        created_data_vars: dict[str, xr.Variable] = {
            "valid_start_datetime": xr.Variable((), np.int64(valid_start_datetime)),
            # "gradiometer_factor": xr.Variable(
            #     ("dim0", "dim0"), np.zeros((3, 3), dtype=np.float64)
            # ),
            # "offsets": xr.Variable(("sensor", "range", "axis"), offsets_data),
            # "spin_average_application_factor": xr.Variable((), np.float64(1.0)),
            # "number_of_spins": xr.Variable((), np.uint32(240)),
            # "quality_flag_threshold": xr.Variable((), np.float64(0.0)),
            "URFTOORFO": xr.Variable(("record0", "dim0", "dim1"), urftoorfo_data),
            "URFTOORFI": xr.Variable(("record0", "dim0", "dim1"), urftoorfi_data),
        }

        dataset = xr.Dataset(
            data_vars=created_data_vars,
            coords=created_coord_vars,
            attrs=gatt,
        )

        logger.info(
            "Using unity matrix (so zero rotation) for calibration application."
        )

        return dataset
