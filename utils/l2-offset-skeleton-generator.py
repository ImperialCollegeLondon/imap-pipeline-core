import datetime

import numpy as np
from spacepy import pycdf

fake_date = "skeleton"

current_date = datetime.date.today().strftime("%Y%m%d")
time = [datetime.datetime(2000, 10, 1, 1, val) for val in range(60)]

data = np.random.random_sample(len(time))

time_shifts = np.random.random_sample(len(time)) / 100

example_offsets = -1 * (12 + (np.random.random_sample((len(time), 3)) - 0.5) * 2)

example_magi_offsets = example_offsets - 10

cdf = pycdf.CDF(f"imap_mag_l2_calibration_{fake_date}_v006.cdf", "")

# Necessary global attributes for ISTP compliance (do calibration files need to be ISTP compliant?)

cdf.attrs["Project"] = "STP>Solar-Terrestrial Physics"
cdf.attrs["Source_name"] = "IMAP"
cdf.attrs["Discipline"] = "Space Physics>Heliospheric Physics"
cdf.attrs["Data_type"] = "L2-calibration>Level-2 calibration parameters"
cdf.attrs["Descriptor"] = "MAG>Magnetometer"
cdf.attrs["Data_version"] = "v006"
cdf.attrs["Generation_date"] = current_date
cdf.attrs["Logical_file_id"] = f"imap_mag_l2_calibration_{fake_date}"
cdf.attrs["Logical_source"] = "imap_mag_l2_calibration"
cdf.attrs["Logical_source_description"] = "Level 2 Calibration Data"
cdf.attrs["Mission_group"] = "IMAP"
cdf.attrs["PI_affiliation"] = "Imperial College London"
cdf.attrs["Parents"] = []
cdf.attrs["TEXT"] = (
    "The IMAP magnetometer requires dynamic calibration to remove the magnetic field of the spacecraft. The offset components should be added to each vector."
)


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

cdf.new(
    "valid_end_datetime",
    datetime.datetime(2025, 1, 1, 0, 0, 0),
    recVary=False,
    type=pycdf.const.CDF_TIME_TT2000,
)
cdf["valid_end_datetime"].attrs["CATDESC"] = (
    "The end time of the validity of this calibration file"
)
cdf["valid_end_datetime"].attrs["VALIDMIN"] = datetime.datetime(1990, 1, 1, 0, 0, 0)
cdf["valid_end_datetime"].attrs["VALIDMAX"] = datetime.datetime(2100, 1, 1, 0, 0, 0)
cdf["valid_end_datetime"].attrs["VAR_TYPE"] = "metadata"

cdf["epoch"] = []
cdf["epoch"].attrs["CATDESC"] = (
    "Time, number of nanoseconds since J2000 with leap seconds included"
)
cdf["epoch"].attrs["VALIDMIN"] = datetime.datetime(1990, 1, 1, 0, 0, 0)
cdf["epoch"].attrs["VALIDMAX"] = datetime.datetime(2100, 1, 1, 0, 0, 0)
cdf["epoch"].attrs["VAR_TYPE"] = "data"

# cdf.new('axis', "", recVary=False)
cdf["axis"] = ["x", "y", "z"]  # or 1 2 3 or something else
cdf["axis"].attrs["CATDESC"] = "Axis of the magnetic field vector"
cdf["axis"].attrs["UNITS"] = " "
cdf["axis"].attrs["VAR_TYPE"] = "data"

cdf.new(
    "offsets",
    [],
    recVary=True,
    type=pycdf.const.CDF_FLOAT,
)
cdf["offsets"].attrs["CATDESC"] = (
    "Offsets to be added to each component of each measurement at each timestamp. These offsets are in the orthogonal sensor reference frame."
)
cdf["offsets"].attrs["UNITS"] = "nT"
cdf["offsets"].attrs["VAR_TYPE"] = "data"
cdf["offsets"].attrs["DEPEND_0"] = "epoch"
cdf["offsets"].attrs["DEPEND_1"] = "axis"

cdf["timedeltas"] = []
cdf["timedeltas"].attrs["CATDESC"] = "Time offsets to be added to each timestamp"
cdf["timedeltas"].attrs["UNITS"] = "s"
cdf["timedeltas"].attrs["VAR_TYPE"] = "data"
cdf["timedeltas"].attrs["DEPEND_0"] = "epoch"

cdf.new("quality_flag", [], type=pycdf.const.CDF_UINT1)
cdf["quality_flag"].attrs["VALIDMIN"] = 0
cdf["quality_flag"].attrs["VALIDMAX"] = 1
cdf["quality_flag"].attrs["CATDESC"] = "Quality flag for measurements at each timestamp"
cdf["quality_flag"].attrs["VAR_TYPE"] = "data"
cdf["quality_flag"].attrs["DEPEND_0"] = "epoch"
cdf["quality_flag"].attrs["VAR_NOTES"] = "0: Good data, 1: Known issues with the data"

cdf.new("quality_bitmask", [], type=pycdf.const.CDF_UINT1)
cdf["quality_bitmask"].attrs["CATDESC"] = (
    "Bitmask denoting the quality of measurements at each timestamp"
)
cdf["quality_bitmask"].attrs["VAR_TYPE"] = "data"
cdf["quality_bitmask"].attrs["DEPEND_0"] = "epoch"
cdf["quality_bitmask"].attrs["VALIDMIN"] = 0
cdf["quality_bitmask"].attrs["VALIDMAX"] = 255
cdf["quality_bitmask"].attrs["VAR_NOTES"] = (
    "Bit 0: Data is sourced from secondary sensor, Bit 1: Large differences between MAGo and MAGi indicative of spacecraft interference, Bit 2: Raised if thruster firing signals have been removed, Bit 3: Raised if instrument signals have been removed, Bits 4-7: Reserved for in flight calibration"
)

cdf.close()
