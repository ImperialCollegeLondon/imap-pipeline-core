from enum import Enum


class Subsystem(Enum):
    """Enum for IMAP subsystems."""

    def __init__(self, short_name: str, tlm_db_file: str) -> None:
        super().__init__()

        # Typer does not support Enums with tuple values,
        # so we need to overwrite the value with a string name
        self._value_ = short_name

        self.short_name = short_name
        self.tlm_db_file = tlm_db_file

    SC = "sc", "sc_4.2_111.xml"
    MAG = "mag", "mag_17.9.xml"
