import logging
from enum import Enum

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

    @classmethod
    def list(cls) -> list[str]:
        """List all HK packets."""
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

        logger.critical(f"ApID {apid} does not match any known packet.")
        raise ValueError(f"ApID {apid} does not match any known packet.")
