from datetime import datetime

import numpy as np


class CONSTANTS:
    IMAP_EPOCH = np.datetime64("2010-01-01T00:00:00", "ns")
    J2000_EPOCH = np.datetime64("2000-01-01T11:58:55.816", "ns")
    J2000_EPOCH_POSIX = datetime(2000, 1, 1, 11, 58, 55, 816000).timestamp()

    MAG_APID_RANGE = (992, 1119)
