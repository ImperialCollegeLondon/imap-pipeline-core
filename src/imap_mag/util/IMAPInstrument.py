from enum import Enum


class IMAPInstrument(str, Enum):
    """Enum for IMAP instruments."""

    SC = "sc"
    MAG = "mag"
    ILO = "ilo"
