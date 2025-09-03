from enum import Enum


class IMAPInstrument(Enum):
    """Enum for IMAP instruments."""

    def __init__(self, short_name: str, version: str) -> None:
        super().__init__()

        # Typer does not support Enums with tuple values,
        # so we need to overwrite the value with a string name
        self._value_ = short_name

        self.short_name = short_name
        self.version = version

    SC = "sc", "4.2"
    MAG = "mag", "17.9"
