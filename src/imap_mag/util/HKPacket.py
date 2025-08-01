import logging
from enum import Enum

from imap_mag.util import CONSTANTS

logger = logging.getLogger(__name__)


class HKPacket(Enum):
    """Enum for HK packets."""

    def __init__(self, apid: int, packet: str, instrument: str) -> None:
        super().__init__()

        # Typer does not support Enums with tuple values,
        # so we need to overwrite the value with a string name
        self._value_ = self.name

        self.apid = apid
        self.packet = packet
        self.instrument = instrument

    # SID packets
    SID1 = 1028, "MAG_HSK_SID1", "mag"
    SID2 = 1055, "MAG_HSK_SID2", "mag"
    SID3_PW = 1063, "MAG_HSK_PW", "mag"
    SID4_STATUS = 1064, "MAG_HSK_STATUS", "mag"
    SID5_SCI = 1082, "MAG_HSK_SCI", "mag"
    SID11_PROCSTAT = 1051, "MAG_HSK_PROCSTAT", "mag"
    SID12 = 1060, "MAG_HSK_SID12", "mag"
    SID15 = 1053, "MAG_HSK_SID15", "mag"
    SID16 = 1054, "MAG_HSK_SID16", "mag"
    SID20 = 1045, "MAG_HSK_SID20", "mag"

    SCID_X285 = 645, "SCID_X285", "sc"
    ILO_APP_NHK = 677, "ILO_APP_NHK", "ilo"

    # All other HK packets
    EHS_AUTONOMY = 1071, "MAG_EHS_AUTONOMY"
    EHS_BTFAIL = 1029, "MAG_EHS_BTFAIL"
    EHS_BUFF = 1079, "MAG_EHS_BUFF"
    EHS_BSW = 1025, "MAG_EHS_BSW"
    EHS_CRSHREP = 1034, "MAG_EHS_CRSHREP"
    EHS_ERRDATA = 1073, "MAG_EHS_ERRDATA"
    EHS_FEE = 1080, "MAG_EHS_FEE"
    EHS_GENEVNT = 1072, "MAG_EHS_GENEVNT"
    EHS_HKADC = 1070, "MAG_EHS_HKADC"
    EHS_OSERR = 1022, "MAG_EHS_OSERR"
    EHS_SEMAPH = 1074, "MAG_EHS_SEMAPH"
    EHS_SWPCKDROP = 1026, "MAG_EHS_SWPCKDROP"
    EHS_SWTRAP = 1067, "MAG_EHS_SWTRAP"
    ELS_CONFLD = 1081, "MAG_ELS_CONFLD"
    ELS_ITF = 1061, "MAG_ELS_ITF"
    HSK_AUTONOMY = 1000, "MAG_HSK_AUTONOMY"
    HSK_HKADCPRMLIM = 1037, "MAG_HSK_HKADCPRMLIM"
    MEM_CHCKREP = 1033, "MAG_MEM_CHCKREP"
    MEM_DMP = 1018, "MAG_MEM_DMP"
    MEM_MRAMTSEGREP = 1038, "MAG_MEM_MRAMTSEGREP"
    PROG_BTSUCC = 1030, "MAG_PROG_BTSUCC"
    PROG_MTRAN = 1024, "MAG_PROG_MTRAN"
    PROG_NOOP = 995, "MAG_PROG_NOOP"
    TCA_INVCCSDS = 1062, "MAG_TCA_INVCCSDS"
    TCA_SUCC = 1058, "MAG_TCA_SUCC"
    TCC_FAIL = 1065, "MAG_TCC_FAIL"
    TCC_FAILMEM = 1066, "MAG_TCC_FAILMEM"
    TCC_FEEFAIL = 1075, "MAG_TCC_FEEFAIL"
    TCC_FILEFAIL = 1078, "MAG_TCC_FILEFAIL"
    TCC_OSFAIL = 1076, "MAG_TCC_OSFAIL"
    TCC_PARAMFAIL = 1077, "MAG_TCC_PARAMFAIL"
    TCC_SPIERR = 1069, "MAG_TCC_SPIERR"
    TCC_SUCC = 1059, "MAG_TCC_SUCC"

    @classmethod
    def all(cls) -> list["HKPacket"]:
        """Get all HK packets."""
        return list(cls)

    @classmethod
    def names(cls) -> list[str]:
        """List all HK packet names."""
        return [e.name for e in cls]

    @classmethod
    def get_all_mag(cls):
        """Get all MAG HK packets."""
        return [e for e in cls if e.instrument == "mag"]

    @classmethod
    def get_all_other(cls):
        """Get all HK packets that are not MAG-specific."""
        return [e for e in cls if e.instrument != "mag"]

    @classmethod
    def from_apid(cls, apid: int) -> "HKPacket":
        """Get HKPacket from APID."""

        for e in cls:
            if e.apid == apid:
                return e

        if apid < CONSTANTS.MAG_APID_RANGE[0] or apid > CONSTANTS.MAG_APID_RANGE[1]:
            logger.critical(
                f"APID {apid} is out of range for MAG HK packets (992-1119)."
            )
            raise ValueError(
                f"APID {apid} is out of range for MAG HK packets (992-1119)."
            )
        else:
            logger.critical(f"ApID {apid} does not match any known MAG HK packet.")
            raise ValueError(f"ApID {apid} does not match any known MAG HK packet.")
