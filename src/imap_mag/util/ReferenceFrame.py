from enum import Enum


class ReferenceFrame(str, Enum):
    DSRF = "dsrf"
    SRF = "srf"
    RTN = "rtn"
    GSE = "gse"
    GSM = "gsm"
