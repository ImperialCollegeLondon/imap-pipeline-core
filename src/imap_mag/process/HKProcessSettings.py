import abc

from imap_mag.util import CONSTANTS, IMAPInstrument


class HKProcessSettings(abc.ABC):
    """Base class for process settings."""

    @staticmethod
    def from_instrument(instrument: IMAPInstrument):
        """Create settings instance from instrument."""
        match instrument:
            case IMAPInstrument.MAG:
                return MAGHKSettings()
            case IMAPInstrument.SC:
                return SCHKSettings()
            case _:
                raise ValueError(f"Unsupported instrument: {instrument.value}")

    @property
    @abc.abstractmethod
    def drop_duplicate_variables(self) -> list[str]:
        """List of variables to drop duplicates on."""
        pass

    @property
    @abc.abstractmethod
    def sort_variables(self) -> list[str]:
        """List of variables to sort by."""
        pass


class MAGHKSettings(HKProcessSettings):
    """Settings for the MAG HK processing."""

    @property
    def drop_duplicate_variables(self) -> list[str]:
        return [
            CONSTANTS.CCSDS_FIELD.APID,
            CONSTANTS.CCSDS_FIELD.SHCOARSE,
            CONSTANTS.CCSDS_FIELD.SEQ_COUNTER,
        ]

    @property
    def sort_variables(self) -> list[str]:
        return [
            CONSTANTS.CCSDS_FIELD.SHCOARSE,
            CONSTANTS.CCSDS_FIELD.SEQ_COUNTER,
        ]


class SCHKSettings(HKProcessSettings):
    """Settings for the S/C HK processing."""

    @property
    def drop_duplicate_variables(self) -> list[str]:
        return [
            CONSTANTS.CCSDS_FIELD.APID,
            CONSTANTS.CCSDS_FIELD.HK_LOW_TPSH_MET_SEC,
            CONSTANTS.CCSDS_FIELD.HK_LOW_TPSH_MET_SUBSEC,
            CONSTANTS.CCSDS_FIELD.SEQ_COUNTER,
        ]

    @property
    def sort_variables(self) -> list[str]:
        return [
            CONSTANTS.CCSDS_FIELD.HK_LOW_TPSH_MET_SEC,
            CONSTANTS.CCSDS_FIELD.HK_LOW_TPSH_MET_SUBSEC,
            CONSTANTS.CCSDS_FIELD.SEQ_COUNTER,
        ]
