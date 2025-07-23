from dataclasses import dataclass

from imap_mag.io.file.SequenceablePathHandler import SequenceablePathHandler


@dataclass
class PartitionedPathHandler(SequenceablePathHandler):
    """
    Interface for path handlers that support parts.

    This class defines the interface for all path handlers that support
    being split into parts.
    """

    part: int = 1

    def get_sequence_variable_name(self) -> str:
        return "part"

    def get_sequence(self) -> int:
        return self.part

    def set_sequence(self, sequence: int) -> None:
        self.part = sequence

    def increase_sequence(self) -> None:
        self.part += 1
