import logging
from enum import Enum

from imap_mag.util.constants import CONSTANTS
from imap_mag.util.Subsystem import Subsystem

logger = logging.getLogger(__name__)


class HKPacket(Enum):
    """Enum for HK packets."""

    def __init__(self, apid: int, packet_name: str, instrument: Subsystem) -> None:
        super().__init__()

        # Typer does not support Enums with tuple values,
        # so we need to overwrite the value with a string name
        self._value_ = self.name

        self.apid = apid
        self.packet_name = packet_name
        self.instrument = instrument

    # MAG SID packets
    SID1 = 1028, "MAG_HSK_SID1", Subsystem.MAG
    SID2 = 1055, "MAG_HSK_SID2", Subsystem.MAG
    SID3_PW = 1063, "MAG_HSK_PW", Subsystem.MAG
    SID4_STATUS = 1064, "MAG_HSK_STATUS", Subsystem.MAG
    SID5_SCI = 1082, "MAG_HSK_SCI", Subsystem.MAG
    SID11_PROCSTAT = 1051, "MAG_HSK_PROCSTAT", Subsystem.MAG
    SID12 = 1060, "MAG_HSK_SID12", Subsystem.MAG
    SID15 = 1053, "MAG_HSK_SID15", Subsystem.MAG
    SID16 = 1054, "MAG_HSK_SID16", Subsystem.MAG
    SID20 = 1045, "MAG_HSK_SID20", Subsystem.MAG

    # All other MAG HK packets
    EHS_AUTONOMY = 1071, "MAG_EHS_AUTONOMY", Subsystem.MAG
    EHS_BTFAIL = 1029, "MAG_EHS_BTFAIL", Subsystem.MAG
    EHS_BUFF = 1079, "MAG_EHS_BUFF", Subsystem.MAG
    EHS_BSW = 1025, "MAG_EHS_BSW", Subsystem.MAG
    EHS_CRSHREP = 1034, "MAG_EHS_CRSHREP", Subsystem.MAG
    EHS_ERRDATA = 1073, "MAG_EHS_ERRDATA", Subsystem.MAG
    EHS_FEE = 1080, "MAG_EHS_FEE", Subsystem.MAG
    EHS_GENEVNT = 1072, "MAG_EHS_GENEVNT", Subsystem.MAG
    EHS_HKADC = 1070, "MAG_EHS_HKADC", Subsystem.MAG
    EHS_OSERR = 1022, "MAG_EHS_OSERR", Subsystem.MAG
    EHS_SEMAPH = 1074, "MAG_EHS_SEMAPH", Subsystem.MAG
    EHS_SWPCKDROP = 1026, "MAG_EHS_SWPCKDROP", Subsystem.MAG
    EHS_SWTRAP = 1067, "MAG_EHS_SWTRAP", Subsystem.MAG
    ELS_CONFLD = 1081, "MAG_ELS_CONFLD", Subsystem.MAG
    ELS_ITF = 1061, "MAG_ELS_ITF", Subsystem.MAG
    HSK_AUTONOMY = 1000, "MAG_HSK_AUTONOMY", Subsystem.MAG
    HSK_HKADCPRMLIM = 1037, "MAG_HSK_HKADCPRMLIM", Subsystem.MAG
    MEM_CHCKREP = 1033, "MAG_MEM_CHCKREP", Subsystem.MAG
    MEM_DMP = 1018, "MAG_MEM_DMP", Subsystem.MAG
    MEM_MRAMTSEGREP = 1038, "MAG_MEM_MRAMTSEGREP", Subsystem.MAG
    PROG_BTSUCC = 1030, "MAG_PROG_BTSUCC", Subsystem.MAG
    PROG_MTRAN = 1024, "MAG_PROG_MTRAN", Subsystem.MAG
    PROG_NOOP = 995, "MAG_PROG_NOOP", Subsystem.MAG
    TCA_INVCCSDS = 1062, "MAG_TCA_INVCCSDS", Subsystem.MAG
    TCA_SUCC = 1058, "MAG_TCA_SUCC", Subsystem.MAG
    TCC_FAIL = 1065, "MAG_TCC_FAIL", Subsystem.MAG
    TCC_FAILMEM = 1066, "MAG_TCC_FAILMEM", Subsystem.MAG
    TCC_FEEFAIL = 1075, "MAG_TCC_FEEFAIL", Subsystem.MAG
    TCC_FILEFAIL = 1078, "MAG_TCC_FILEFAIL", Subsystem.MAG
    TCC_OSFAIL = 1076, "MAG_TCC_OSFAIL", Subsystem.MAG
    TCC_PARAMFAIL = 1077, "MAG_TCC_PARAMFAIL", Subsystem.MAG
    TCC_SPIERR = 1069, "MAG_TCC_SPIERR", Subsystem.MAG
    TCC_SUCC = 1059, "MAG_TCC_SUCC", Subsystem.MAG

    # S/C HK packets
    SCID_X17C = 380, "SCID_X17C", Subsystem.SC  # file status
    SCID_X1DC = 476, "SCID_X1DC", Subsystem.SC  # RT & TTGS CMD
    SCID_X1DF = 479, "SCID_X1DF", Subsystem.SC  # C&SM
    SCID_X285 = 645, "SCID_X285", Subsystem.SC  # currents
    SCID_X286 = 646, "SCID_X286", Subsystem.SC  # counters
    SCGLOBAL = 2047, "SCGLOBAL", Subsystem.SC  # temperatures

    @classmethod
    def names(cls) -> list[str]:
        """List all HK packet names."""
        return [hk.name for hk in HKPacket]

    @classmethod
    def apids(cls) -> list[int]:
        """List all HK packet APIDs."""
        return [hk.apid for hk in HKPacket]

    @classmethod
    def from_apid(cls, apid: int) -> "HKPacket":
        """Get HKPacket from ApID."""

        for hk in cls:
            if hk.apid == apid:
                return hk

        if apid < CONSTANTS.MAG_APID_RANGE[0] or apid > CONSTANTS.MAG_APID_RANGE[1]:
            logger.critical(
                f"ApID {apid} is out of range for MAG HK packets (992-1119)."
            )
            raise ValueError(
                f"ApID {apid} is out of range for MAG HK packets (992-1119)."
            )
        else:
            logger.critical(f"ApID {apid} does not match any known HK packet.")
            raise ValueError(f"ApID {apid} does not match any known HK packet.")

    @classmethod
    def from_name(cls, packet_name: str) -> "HKPacket":
        """Get HKPacket from packet name."""

        try:
            return cls[packet_name]
        except KeyError as exception:
            logger.error(
                f"Packet name {packet_name} does not match any known HK packet."
            )
            raise ValueError(
                f"Packet name {packet_name} does not match any known HK packet."
            ) from exception
