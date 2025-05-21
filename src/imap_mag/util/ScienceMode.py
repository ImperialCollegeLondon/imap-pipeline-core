from enum import Enum


class ScienceMode(Enum):
    def __init__(self, short_name: str, apid: int, packet: str) -> None:
        super().__init__()

        # Typer does not support Enums with tuple values,
        # so we need to overwrite the value with a string name
        self._value_ = short_name

        self.short_name = short_name
        self.apid = apid
        self.packet = packet

    Normal = "norm", 1052, "MAG_SCI_NORM"
    Burst = "burst", 1068, "MAG_SCI_BURST"
