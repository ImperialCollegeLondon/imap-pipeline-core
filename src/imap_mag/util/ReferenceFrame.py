from enum import Enum


class ReferenceFrame(Enum, str):
    DSRF = "dsrf"
    SRF = "srf"
    RTN = "rtn"
    GSE = "gse"
    GSM = "gsm"
