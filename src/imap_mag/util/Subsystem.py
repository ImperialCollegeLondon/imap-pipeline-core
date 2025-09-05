from enum import Enum


class Subsystem(Enum):
    """Enum for IMAP subsystems."""

    def __init__(self, short_name: str, tlm_db_version: str) -> None:
        super().__init__()

        # Typer does not support Enums with tuple values,
        # so we need to overwrite the value with a string name
        self._value_ = short_name

        self.short_name = short_name
        self.tlm_db_version = tlm_db_version

    SC = "sc", "4.2"
    MAG = "mag", "17.9"
