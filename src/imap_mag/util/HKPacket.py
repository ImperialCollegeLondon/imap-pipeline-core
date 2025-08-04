import abc
import logging
import typing
from enum import Enum, EnumMeta

from imap_mag.util import CONSTANTS
from imap_mag.util.IMAPInstrument import IMAPInstrument

logger = logging.getLogger(__name__)

T = typing.TypeVar("T", bound="HKPacket")


class HKPacketMetaClass(abc.ABCMeta, EnumMeta):
    pass


class HKPacket(abc.ABC, Enum, metaclass=HKPacketMetaClass):
    def __init__(self, apid: int, packet: str) -> None:
        super().__init__()

        # Typer does not support Enums with tuple values,
        # so we need to overwrite the value with a string name
        self._value_ = self.name

        self.apid = apid
        self.packet = packet

    @property
    @abc.abstractmethod
    def instrument(self) -> IMAPInstrument:
        """Return the instrument associated with this HK packet."""
        pass

    @classmethod
    def all(cls: type[T]) -> list[T]:
        """Get all HK packets."""

        if cls is HKPacket:
            all = []
            for hk_type in cls.__subclasses__():
                all.extend(hk_type.all())
            return all
        else:
            return list(cls)

    @classmethod
    def names(cls: type[T]) -> list[str]:
        """List all HK packet names."""
        return [e.name for e in cls.all()]

    @classmethod
    def from_any_apid(cls, apid: int) -> "HKPacket":
        """Get HKPacket from APID."""

        for hk_type in cls.__subclasses__():
            try:
                return hk_type.from_apid(apid)
            except ValueError:
                continue

        logger.error(f"ApID {apid} does not match any known HK packet.")
        raise ValueError(f"ApID {apid} does not match any known HK packet.")

    @classmethod
    def from_any_name(cls, packet_name: str) -> "HKPacket":
        """Get HKPacket from packet name."""

        for hk_type in cls.__subclasses__():
            try:
                return hk_type[packet_name]
            except KeyError:
                continue

        logger.error(f"Packet name {packet_name} does not match any known HK packet.")
        raise ValueError(
            f"Packet name {packet_name} does not match any known HK packet."
        )

    @classmethod
    @abc.abstractmethod
    def from_apid(cls: type[T], apid: int) -> T:
        """Get HKPacket from APID."""
        pass


class MAGPacket(HKPacket):
    """Enum for MAG HK packets."""

    # SID packets
    SID1 = 1028, "MAG_HSK_SID1"
    SID2 = 1055, "MAG_HSK_SID2"
    SID3_PW = 1063, "MAG_HSK_PW"
    SID4_STATUS = 1064, "MAG_HSK_STATUS"
    SID5_SCI = 1082, "MAG_HSK_SCI"
    SID11_PROCSTAT = 1051, "MAG_HSK_PROCSTAT"
    SID12 = 1060, "MAG_HSK_SID12"
    SID15 = 1053, "MAG_HSK_SID15"
    SID16 = 1054, "MAG_HSK_SID16"
    SID20 = 1045, "MAG_HSK_SID20"

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

    @property
    def instrument(self) -> IMAPInstrument:
        return IMAPInstrument.MAG

    @classmethod
    def from_apid(cls, apid: int) -> "MAGPacket":
        """Get MAGPacket from APID."""

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


class SpacecraftPacket(HKPacket):
    """Enum for Spacecraft HK packets."""

    SCID_X285 = 645, "SCID_X285"

    @property
    def instrument(self) -> IMAPInstrument:
        return IMAPInstrument.SC

    @classmethod
    def from_apid(cls, apid: int) -> "SpacecraftPacket":
        """Get SpacecraftPacket from APID."""

        for e in cls:
            if e.apid == apid:
                return e

        logger.critical(f"ApID {apid} does not match any known S/C HK packet.")
        raise ValueError(f"ApID {apid} does not match any known S/C HK packet.")


class ILOPacket(HKPacket):
    """Enum for ILO HK packets."""

    ILO_APP_NHK = 677, "ILO_APP_NHK"

    @property
    def instrument(self) -> IMAPInstrument:
        return IMAPInstrument.ILO

    @classmethod
    def from_apid(cls, apid: int) -> "ILOPacket":
        """Get ILOPacket from APID."""

        for e in cls:
            if e.apid == apid:
                return e

        logger.critical(f"ApID {apid} does not match any known ILO HK packet.")
        raise ValueError(f"ApID {apid} does not match any known ILO HK packet.")
