from datetime import datetime

import numpy as np


class CONSTANTS:
    IMAP_EPOCH = np.datetime64("2010-01-01T00:00:00", "ns")
    J2000_EPOCH = np.datetime64("2000-01-01T11:58:55.816", "ns")
    J2000_EPOCH_POSIX = datetime(2000, 1, 1, 11, 58, 55, 816000).timestamp()

    MAG_APID_RANGE = (992, 1119)
    IALIRT_PACKET_DEFINITION_FILE = "ialirt_4.05.yaml"

    class CCSDS_FIELD:
        APID = "pkt_apid"
        SEQ_COUNTER = "src_seq_ctr"
        SHCOARSE = "shcoarse"
        EPOCH = "epoch"

    class ENV_VAR_NAMES:
        IALIRT_AUTH_CODE = "IALIRT_API_KEY"
        IALIRT_URL = "IALIRT_DATA_ACCESS_URL"
        WEBPODA_AUTH_CODE = "IMAP_WEBPODA_TOKEN"
        SDC_AUTH_CODE = "IMAP_API_KEY"
        SDC_URL = "IMAP_DATA_ACCESS_URL"
