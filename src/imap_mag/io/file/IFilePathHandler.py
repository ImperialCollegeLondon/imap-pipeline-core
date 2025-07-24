import abc
import logging
import typing
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

T = typing.TypeVar("T", bound="IFilePathHandler")


@dataclass
class IFilePathHandler(abc.ABC):
    """
    Interface for path handlers.

    This class defines the interface for all file path handlers.
    The path handlers can be used to manage file I/O operations,
    including folder structure and file naming conventions.
    """

    @abc.abstractmethod
    def supports_sequencing(self) -> bool:
        """Denotes whether this path handler supports sequence-like indexes."""
        pass

    @abc.abstractmethod
    def get_folder_structure(self) -> str:
        pass

    @abc.abstractmethod
    def get_filename(self) -> str:
        pass

    @classmethod
    @abc.abstractmethod
    def from_filename(cls: type[T], filename: str | Path) -> T | None:
        """Instantiate a path handler from a file name."""
        pass

    def _check_property_values(
        self, method_description: str, properties: list[str]
    ) -> None:
        """
        Check if the required properties are set for a method.

        Raises ValueError if any of the properties are not set.
        """

        missing_properties = [
            f"'{prop}'" for prop in properties if not getattr(self, prop)
        ]

        if missing_properties:
            message = f"No {', '.join(missing_properties)} defined. Cannot generate {method_description}."

            logger.error(message)
            raise ValueError(message)
