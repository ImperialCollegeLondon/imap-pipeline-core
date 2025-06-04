import abc

from imap_db.model import Base, DownloadProgress, File


class IDatabase(abc.ABC):
    """Interface for database manager."""

    def insert_file(self, file: File) -> None:
        """Insert a file into the database."""
        self.insert_files([file])
        pass

    @abc.abstractmethod
    def insert_files(self, files: list[File]) -> None:
        """Insert a list of files into the database."""
        pass

    @abc.abstractmethod
    def get_download_progress(self, item_name: str) -> DownloadProgress:
        """Get the progress timestamp for an item."""
        pass

    @abc.abstractmethod
    def get_files(self, *args, **kwargs) -> list[File]:
        """Get a list of files from the database with optional filters."""
        pass

    @abc.abstractmethod
    def save(self, model: Base) -> None:
        """Save an object to the database."""
        pass
