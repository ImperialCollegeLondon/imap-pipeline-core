import datetime

import numpy as np
from spacepy import pycdf

current_date = datetime.date.today().strftime("%Y%m%d")

cdf = pycdf.CDF("imap_mag_l1d_calibration_20260102_v000.cdf", "")

frame_transform_magi = np.ones((3, 3, 4))

print(frame_transform_magi)

frame_transform_mago = np.ones((3, 3, 4))

# Necessary global attributes for ISTP compliance (do calibration files need to be ISTP compliant?)

cdf.attrs["Project"] = "STP>Solar-Terrestrial Physics"
cdf.attrs["Source_name"] = "IMAP"
cdf.attrs["Discipline"] = "Space Physics>Heliospheric Physics"
cdf.attrs["Data_type"] = "L1D-calibration>Level-1D calibration parameters"
cdf.attrs["Descriptor"] = "MAG>Magnetometer"
cdf.attrs["Data_version"] = "v000"
cdf.attrs["Generation_date"] = current_date
cdf.attrs["Logical_file_id"] = "imap_mag_l1d_calibration_20260101"
cdf.attrs["Logical_source"] = "imap_mag_l1d_calibration"
cdf.attrs["Logical_source_description"] = "Level 1D Calibration Data"
cdf.attrs["Mission_group"] = "IMAP"
cdf.attrs["PI_affiliation"] = "Imperial College London"
cdf.attrs["TEXT"] = (
    "The IMAP magnetometer requires dynamic calibration to remove the magnetic field of the spacecraft. The calibration parameters should be applied to each vector according to its sensor."
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

cdf.new("ValidStartDatetime", "", recVary=False)
cdf["ValidStartDatetime"].attrs["CATDESC"] = (
    "The start time of the validity of this calibration file"
)
cdf["ValidStartDatetime"].attrs["VALIDMIN"] = datetime.datetime(1990, 1, 1, 0, 0, 0)
cdf["ValidStartDatetime"].attrs["VALIDMAX"] = datetime.datetime(2100, 1, 1, 0, 0, 0)
cdf["ValidStartDatetime"].attrs["VAR_TYPE"] = "metadata"

cdf.new("ValidEndDatetime", "", recVary=False)
cdf["ValidEndDatetime"].attrs["CATDESC"] = (
    "The end time of the validity of this calibration file"
)
cdf["ValidEndDatetime"].attrs["VALIDMIN"] = datetime.datetime(1990, 1, 1, 0, 0, 0)
cdf["ValidEndDatetime"].attrs["VALIDMAX"] = datetime.datetime(2100, 1, 1, 0, 0, 0)
cdf["ValidEndDatetime"].attrs["VAR_TYPE"] = "metadata"

cdf.new("GradiometerFactor", 0, recVary=False)
cdf["GradiometerFactor"].attrs["CATDESC"] = (
    "The end time of the validity of this calibration file"
)
cdf["GradiometerFactor"].attrs["VALIDMIN"] = 0
cdf["GradiometerFactor"].attrs["VALIDMAX"] = 1
cdf["GradiometerFactor"].attrs["VAR_TYPE"] = "metadata"

cdf["Offsets"] = []
cdf["Offsets"].attrs["CATDESC"] = (
    "Offsets to be added to each sensor at each range. These offsets are in the reference frame of the sensor."
)
cdf["Offsets"].attrs["UNITS"] = "nT"
cdf["Offsets"].attrs["VAR_TYPE"] = "data"
cdf["Offsets"].attrs["DEPEND_0"] = "range"
cdf["Offsets"].attrs["DEPEND_1"] = "sensor"

cdf["Timedeltas"] = []
cdf["Timedeltas"].attrs["CATDESC"] = "Time offsets for each sensor"
cdf["Timedeltas"].attrs["UNITS"] = "ms"
cdf["Timedeltas"].attrs["VAR_TYPE"] = "data"
cdf["Timedeltas"].attrs["DEPEND_0"] = "sensor"

cdf["URFTOORFO"] = frame_transform_mago
cdf["URFTOORFO"].attrs["CATDESC"] = "Frame transform from URF to ORF for MAGo"
cdf["URFTOORFO"].attrs["UNITS"] = " "
cdf["URFTOORFO"].attrs["DEPEND_0"] = "range"

cdf["URFTOORFI"] = [frame_transform_mago, frame_transform_magi]
cdf["URFTOORFI"].attrs["CATDESC"] = "Frame transform to be applied to each sensor"
cdf["URFTOORFI"].attrs["UNITS"] = " "
cdf["URFTOORFI"].attrs["DEPEND_0"] = "range"


cdf.close()
