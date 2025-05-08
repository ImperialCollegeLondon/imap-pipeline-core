import logging
from enum import Enum

logger = logging.getLogger(__name__)


class HKPacket(Enum):
    """Enum for HK packets."""

    def __init__(self, apid: int, packet: str) -> None:
        super().__init__()

        # Typer does not support Enums with tuple values,
        # so we need to overwrite the value with a string name
        self._value_ = self.name

        self.apid = apid
        self.packet = packet

    SID1 = 1028, "MAG_HSK_SID1"
    SID2 = 1055, "MAG_HSK_SID2"
    PW = 1063, "MAG_HSK_PW"
    STATUS = 1064, "MAG_HSK_STATUS"
    SCI = 1082, "MAG_HSK_SCI"
    PROCSTAT = 1051, "MAG_HSK_PROCSTAT"
    SID12 = 1060, "MAG_HSK_SID12"
    SID15 = 1053, "MAG_HSK_SID15"
    SID16 = 1054, "MAG_HSK_SID16"
    SID20 = 1045, "MAG_HSK_SID20"

    @classmethod
    def list(cls) -> list[str]:
        """List all HK packets."""
        return [e.name for e in cls]

    @classmethod
    def from_apid(cls, apid: int) -> "HKPacket":
        """Get HKPacket from APID."""

        for e in cls:
            if e.apid == apid:
                return e

        logger.critical(f"ApID {apid} does not match any known packet.")
        raise ValueError(f"ApID {apid} does not match any known packet.")
