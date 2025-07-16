import datetime

import numpy as np
from spacepy import pycdf

current_date = datetime.date.today().strftime("%Y%m%d")
time = [datetime.datetime(2000, 10, 1, 1, val) for val in range(60)]

data = np.random.random_sample(len(time))

time_shifts = np.random.random_sample(len(time)) / 100

example_offsets = -1 * (12 + (np.random.random_sample((len(time), 3)) - 0.5) * 2)

example_magi_offsets = example_offsets - 10

cdf = pycdf.CDF("imap_mag_l2_calibration_20260102_v000.cdf", "")

frame_transform_magi = np.ones((3, 3))

frame_transform_mago = np.ones((3, 3))

# Necessary global attributes for ISTP compliance (do calibration files need to be ISTP compliant?)

cdf.attrs["Project"] = "STP>Solar-Terrestrial Physics"
cdf.attrs["Source_name"] = "IMAP"
cdf.attrs["Discipline"] = "Space Physics>Heliospheric Physics"
cdf.attrs["Data_type"] = "L2-calibration>Level-2 calibration parameters"
cdf.attrs["Descriptor"] = "MAG>Magnetometer"
cdf.attrs["Data_version"] = "v000"
cdf.attrs["Generation_date"] = current_date
cdf.attrs["Logical_file_id"] = "imap_mag_l2_calibration_20260102"
cdf.attrs["Logical_source"] = "imap_mag_l2_calibration"
cdf.attrs["Logical_source_description"] = "Level 2 Calibration Data"
cdf.attrs["Mission_group"] = "IMAP"
cdf.attrs["PI_affiliation"] = "Imperial College London"
cdf.attrs["Parents"] = []
cdf.attrs["TEXT"] = (
    "The IMAP magnetometer requires dynamic calibration to remove the magnetic field of the spacecraft. The calibration parameters should be applied to each vector according to its sensor."
)


cdf.new("ValidStartDatetime", "", recVary=False)
cdf["ValidStartDatetime"].attrs["CATDESC"] = (
    "The start time of the validity of this calibration file"
)
cdf["ValidStartDatetime"].attrs["VALIDMIN"] = datetime.datetime(1990, 1, 1, 0, 0, 0)
cdf["ValidStartDatetime"].attrs["VALIDMAX"] = datetime.datetime(2100, 1, 1, 0, 0, 0)
cdf["ValidStartDatetime"].attrs["VAR_TYPE"] = "metadata"

cdf.new("ValidEndDatetime", "", recVary=False)
cdf["ValidEndDatetime"] = ""
cdf["ValidEndDatetime"].attrs["CATDESC"] = (
    "The end time of the validity of this calibration file"
)
cdf["ValidEndDatetime"].attrs["VALIDMIN"] = datetime.datetime(1990, 1, 1, 0, 0, 0)
cdf["ValidEndDatetime"].attrs["VALIDMAX"] = datetime.datetime(2100, 1, 1, 0, 0, 0)
cdf["ValidEndDatetime"].attrs["VAR_TYPE"] = "metadata"


cdf["epoch"] = time
cdf["epoch"].attrs["CATDESC"] = (
    "Time, number of nanoseconds since J2000 with leap seconds included"
)
cdf["epoch"].attrs["VALIDMIN"] = datetime.datetime(1990, 1, 1, 0, 0, 0)
cdf["epoch"].attrs["VALIDMAX"] = datetime.datetime(2100, 1, 1, 0, 0, 0)
cdf["epoch"].attrs["VAR_TYPE"] = "data"

cdf["axis"] = ["x", "y", "z"]  # or 1 2 3 or something else
cdf["axis"].attrs["UNITS"] = ""
cdf["axis"].attrs["VAR_TYPE"] = "data"

cdf["sensor"] = ["MAGo", "MAGi"]
cdf["sensor"].attrs["UNITS"] = ""
cdf["sensor"].attrs["VAR_TYPE"] = "support_data"

cdf["Offsets"] = []
cdf["Offsets"].attrs["CATDESC"] = (
    "Offsets to be added to each sensor measurement at each timestamp. These offsets are in the reference frame of the sensor."
)
cdf["Offsets"].attrs["UNITS"] = "nT"
cdf["Offsets"].attrs["VAR_TYPE"] = "data"
cdf["Offsets"].attrs["DEPEND_0"] = "epoch"
cdf["Offsets"].attrs["DEPEND_1"] = "sensor"

cdf["Timedeltas"] = []
cdf["Timedeltas"].attrs["CATDESC"] = "Time offsets to be added to each timestamp"
cdf["Timedeltas"].attrs["UNITS"] = "ms"
cdf["Timedeltas"].attrs["VAR_TYPE"] = "data"
cdf["Timedeltas"].attrs["DEPEND_0"] = "epoch"

cdf["QUALITY_FLAG"] = []
cdf["QUALITY_FLAG"].attrs["CATDESC"] = (
    "Quality flag for MAGo measurements at each timestamp"
)
cdf["QUALITY_FLAG"].attrs["VAR_TYPE"] = "metadata"
cdf["QUALITY_FLAG"].attrs["DEPEND_0"] = "epoch"

cdf["QUALITY_BITMASK"] = []
cdf["QUALITY_BITMASK"].attrs["CATDESC"] = (
    "Bitmask denoting the quality of MAGo measurements at each timestamp"
)
cdf["QUALITY_BITMASK"].attrs["VAR_TYPE"] = "metadata"
cdf["QUALITY_BITMASK"].attrs["DEPEND_0"] = "epoch"


cdf.close()
