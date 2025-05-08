import abc
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class IFileMetadataProvider(abc.ABC):
    """Interface for metadata providers."""

    version: int = 0

    @abc.abstractmethod
    def supports_versioning(self) -> bool:
        """Check if metadata provider supports versioning."""

    @abc.abstractmethod
    def get_folder_structure(self) -> str:
        """Retrieve folder structure."""

    @abc.abstractmethod
    def get_filename(self) -> str:
        """Retireve file name."""
