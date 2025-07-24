import abc
from pathlib import Path

from imap_mag.io.file.IFilePathHandler import IFilePathHandler


class FileProcessor(abc.ABC):
    """Interface for IMAP processing."""

    @abc.abstractmethod
    def is_supported(self, file: Path) -> bool:
        """Check if the file is supported by this processor."""
        pass

    @abc.abstractmethod
    def initialize(self, packet_definition: Path) -> None:
        pass

    @abc.abstractmethod
    def process(self, files: Path | list[Path]) -> dict[Path, IFilePathHandler]:
        """Process a file or a list of files."""
        pass
