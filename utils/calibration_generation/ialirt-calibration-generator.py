import datetime

import numpy as np
from spacepy import pycdf

current_date = datetime.date.today().strftime("%Y%m%d")

version = "007"

cdf = pycdf.CDF(f"imap_mag_ialirt-calibration_20250627_v{version}.cdf", "")

i = np.eye(3, 3)
frame_transform_magi = np.stack([i, i, i, i], axis=2)

frame_transform_mago = np.stack([i, i, i, i], axis=2)

offsets = np.zeros((2, 4, 3))

# Necessary global attributes for ISTP compliance (do calibration files need to be ISTP compliant?)

cdf.attrs["Project"] = "STP>Solar-Terrestrial Physics"
cdf.attrs["Source_name"] = "IMAP"
cdf.attrs["Discipline"] = "Space Physics>Heliospheric Physics"
cdf.attrs["Data_type"] = "I-ALiRT-calibration>I-ALiRT calibration parameters"
cdf.attrs["Descriptor"] = "MAG>Magnetometer"
cdf.attrs["Data_version"] = version
cdf.attrs["Generation_date"] = current_date
cdf.attrs["Logical_file_id"] = "imap_mag_ialirt-calibration_20250627"
cdf.attrs["Logical_source"] = "imap_mag_ialirt-calibration"
cdf.attrs["Logical_source_description"] = "I-ALiRT Calibration Data"
cdf.attrs["Mission_group"] = "IMAP"
cdf.attrs["PI_affiliation"] = "Imperial College London"
cdf.attrs["TEXT"] = (
    "The IMAP magnetometer sends an I-ALiRT vector every four seconds. The magnetometer vectors require dynamic calibration to remove the magnetic field of the spacecraft. The calibration matrices should be applied based on sensor and range and the offsets added based on sensor and range. The gradiometer factor should be used to calculate dynamic offsets via gradiometry during the calibration process."
)

cdf["axis"] = ["x", "y", "z"]  # or 1 2 3 or something else
cdf["axis"].attrs["UNITS"] = ""
cdf["axis"].attrs["VAR_TYPE"] = "data"

cdf["sensor"] = ["MAGo", "MAGi"]
cdf["sensor"].attrs["UNITS"] = ""
cdf["sensor"].attrs["VAR_TYPE"] = "support_data"

cdf["range"] = [0, 1, 2, 3]
cdf["range"].attrs["UNITS"] = ""
cdf["range"].attrs["VAR_TYPE"] = "support_data"

cdf.new(
    "valid_start_datetime",
    datetime.datetime(2025, 1, 1, 0, 0, 0),
    recVary=False,
    type=pycdf.const.CDF_TIME_TT2000,
)
cdf["valid_start_datetime"].attrs["CATDESC"] = (
    "The start time of the validity of this calibration file"
)
cdf["valid_start_datetime"].attrs["VALIDMIN"] = datetime.datetime(1990, 1, 1, 0, 0, 0)
cdf["valid_start_datetime"].attrs["VALIDMAX"] = datetime.datetime(2100, 1, 1, 0, 0, 0)
cdf["valid_start_datetime"].attrs["VAR_TYPE"] = "metadata"

cdf.new("gradiometer_factor", 0, type=pycdf.const.CDF_FLOAT, recVary=False)
cdf["gradiometer_factor"].attrs["CATDESC"] = (
    "The gradiometer factor to be used for calibrations"
)
cdf["gradiometer_factor"].attrs["VALIDMIN"] = -1
cdf["gradiometer_factor"].attrs["VALIDMAX"] = 1
cdf["gradiometer_factor"].attrs["VAR_TYPE"] = "metadata"

cdf.new(
    "offsets",
    offsets,
    recVary=True,
    type=pycdf.const.CDF_FLOAT,
)
cdf["offsets"].attrs["CATDESC"] = (
    "Offsets to be added to each sensor at each range. These offsets are in the orthogonal reference frame (ORF)."
)
cdf["offsets"].attrs["UNITS"] = "nT"
cdf["offsets"].attrs["VAR_TYPE"] = "data"
cdf["offsets"].attrs["DEPEND_0"] = "sensor"
cdf["offsets"].attrs["DEPEND_1"] = "range"
cdf["offsets"].attrs["DEPEND_2"] = "axis"

cdf["URFTOORFO"] = frame_transform_mago
cdf["URFTOORFO"].attrs["CATDESC"] = "Frame transform from URF to ORF for MAGo"
cdf["URFTOORFO"].attrs["UNITS"] = " "
cdf["URFTOORFO"].attrs["DEPEND_0"] = "range"

cdf["URFTOORFI"] = frame_transform_magi
cdf["URFTOORFI"].attrs["CATDESC"] = "Frame transform from URF to ORF for MAGi"
cdf["URFTOORFI"].attrs["UNITS"] = " "
cdf["URFTOORFI"].attrs["DEPEND_0"] = "range"


cdf.close()
