from __future__ import annotations

import logging
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Self

from sqlalchemy import JSON, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func

from imap_mag import __version__
from imap_mag.util import DatetimeProvider

if TYPE_CHECKING:
    from imap_mag.config.AppSettings import AppSettings

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class File(Base):
    __tablename__ = "files"
    __table_args__ = (
        UniqueConstraint(
            "descriptor",
            "content_date",
            "version",
            "deletion_date",
            name="uq_files_descriptor_content_date_version_deletion_date",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128))
    path: Mapped[str] = mapped_column(String(256), unique=True)

    descriptor: Mapped[str] = mapped_column(String(128))
    version: Mapped[int] = mapped_column(Integer())
    version_major: Mapped[int] = mapped_column(Integer(), default=0, server_default="0")
    hash: Mapped[str] = mapped_column(String(64))
    size: Mapped[int] = mapped_column(Integer())
    content_date: Mapped[datetime] = mapped_column(DateTime(), nullable=True)
    creation_date: Mapped[datetime] = mapped_column(
        DateTime(), server_default=func.now()
    )
    last_modified_date: Mapped[datetime] = mapped_column(
        DateTime(),
        onupdate=func.now(),
        server_default=func.now(),
    )
    deletion_date: Mapped[datetime | None] = mapped_column(DateTime(), nullable=True)
    software_version: Mapped[str] = mapped_column(String(16))
    file_meta: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    def __repr__(self) -> str:
        return f"<File {self.id} (name={self.name}, path={self.path})>"

    def set_deleted(
        self, datetime_provider: DatetimeProvider = DatetimeProvider()
    ) -> None:
        now = datetime_provider.now()
        self.deletion_date = now
        self.last_modified_date = now
        self.software_version = __version__

    def get_full_path(self, settings: AppSettings) -> Path:
        full_path = Path(self.path)
        if not full_path.is_absolute():
            return settings.data_store / full_path

        return full_path

    def merge_record(self, new_file: Self) -> bool:
        """Merge this file record with a new copy of the file. Returns true if an update was made, false if the existing record is unchanged."""
        if self.path != new_file.path or self.name != new_file.name:
            raise ValueError(
                f"Attempting to merge file {self.path} with a different file {new_file.path}"
            )

        if (
            self.hash == new_file.hash
            and self.size == new_file.size
            and self.file_meta == new_file.file_meta
            and self.deletion_date == new_file.deletion_date
            and self.descriptor == new_file.descriptor
            and self.content_date == new_file.content_date
            and self.version == new_file.version
            and self.version_major == new_file.version_major
        ):
            logger.info(f"File record is identical - no updates needed for {self.path}")
            return False

        action = "Updating db record"
        if self.deletion_date is None and new_file.deletion_date is not None:
            action = "Set db record deleted"
        if self.deletion_date is not None and new_file.deletion_date is None:
            action = "Undeleting db record"

        logger.info(
            f"File {new_file.path} record being merged with existing record {self.id}. {action}."
        )

        self.hash = new_file.hash
        self.size = new_file.size
        self.file_meta = new_file.file_meta
        self.deletion_date = new_file.deletion_date
        self.descriptor = new_file.descriptor
        self.content_date = new_file.content_date
        self.version = new_file.version
        self.version_major = new_file.version_major

        now = DatetimeProvider().now()
        self.last_modified_date = now
        self.software_version = __version__
        return True

    def archive_to_new_file_path(self, new_path: Path) -> Self:
        """Create a new File object for the archived file and mark this file as deleted.

        Args:
            new_path: Path where the archived file will be stored
        Returns:         New File object representing the archived file
        """

        archived_file = self.__class__(
            name=self.name,
            path=new_path.as_posix(),
            descriptor=self.descriptor,
            version=self.version,
            version_major=self.version_major,
            hash=self.hash,
            size=self.size,
            content_date=self.content_date,
            creation_date=self.creation_date,
            software_version=self.software_version,
            file_meta=self.file_meta,
        )

        self.set_deleted()
        return archived_file

    @classmethod
    def get_descriptor_from_filename(cls, name: str) -> str:
        """Extract file type string (descriptor) from a filename.

        Examples:
            imap_mag_l1_hsk-status_20251201_v001.csv -> imap_mag_l1_hsk-status
            imap_mag_l2-burst-offsets_20250421_20250421_v000.cdf -> imap_mag_l2-burst-offsets
            imap_mag_l1d_burst-srf_20251207_v001.cdf -> imap_mag_l1d_burst-srf
        """

        name_without_extension = name.rsplit(".", 1)[0] if "." in name else name

        # Match everything up to the last date/version pattern
        # This regex captures everything before date patterns (YYYYMMDD or YYYY-MM-DD) and version patterns (vNNN or NNN)
        match = re.match(
            r"^(.+?)(?:_(?:\d{8}|\d{4}-\d{2}-\d{2}|v\d+(?:\.\d+)?|\d+))+$",
            name_without_extension,
        )
        if match:
            return match.group(1)

        # Fallback: return filename without extension if no pattern matched
        return name_without_extension

    @classmethod
    def from_file(
        cls,
        file: Path,
        version: int,
        hash: str | None,
        content_date: datetime | None,
        settings: AppSettings,
        version_major: int = 0,
    ) -> File:
        if not file.exists():
            raise FileNotFoundError(f"File {file} does not exist.")

        if hash is None or hash == "":
            from imap_mag.io.file import IFilePathHandler

            hash = IFilePathHandler.default_file_hash(file)

        size = file.stat().st_size

        file_with_datastore_relative_path = cls.get_datastore_relative_path(
            file, settings
        )

        return cls(
            name=file.name,
            path=file_with_datastore_relative_path,
            descriptor=cls.get_descriptor_from_filename(file.name),
            version=version,
            version_major=version_major,
            hash=hash,
            size=size,
            content_date=content_date,
            creation_date=datetime.fromtimestamp(file.stat().st_ctime),
            software_version=__version__,
        )

    @classmethod
    def get_datastore_relative_path(
        cls, file: Path, settings: AppSettings, warn: bool = True
    ) -> str:
        try:
            file_with_datastore_relative_path = file.absolute().relative_to(
                settings.data_store.absolute()
            )
        # match exception by message text "not a subpath"
        except ValueError as e:
            if "is not in the subpath of" in str(e):
                if warn:
                    logger.warning(
                        f"File {file} is not within the data store path {settings.data_store}"
                    )
                file_with_datastore_relative_path = file
            else:
                raise
        return file_with_datastore_relative_path.as_posix()

    @classmethod
    def filter_to_latest_versions_only(
        cls, files: list[Self], version_major: int | None = None
    ) -> list[Self]:
        """
        Select only the latest version of files per day.

        Groups files by date and selects the file with the highest version number
        for each date. Files without dates are kept separate and the latest version
        among them is selected.

        Args:
            files: List of File objects from database
            version_major: If provided, pre-filter to only files with this major version

        Returns:
            List of File objects containing only the latest version per day
        """

        if version_major is not None:
            files = [f for f in files if f.version_major == version_major]

        # Group files by type and date so we have lists with each version of the file
        files_by_date: dict[str, list[tuple[Self, int]]] = defaultdict(list)

        for file in files:
            files_by_date[
                f"{file.descriptor}-{file.content_date.date() if file.content_date else 'no_date'}"
            ].append((file, file.version))

        # Select latest version per date
        latest_files = []
        for _, file_list in files_by_date.items():
            # Sort by version (descending) and take the first one
            file_list.sort(key=lambda x: x[1], reverse=True)
            latest_files.append(file_list[0][0])  # Append the file object

        return latest_files


class WorkflowProgress(Base):
    __tablename__ = "workflow_progress"

    item_name: Mapped[str] = mapped_column(String(32), primary_key=True, unique=True)
    progress_timestamp: Mapped[datetime] = mapped_column(DateTime(), nullable=True)
    last_checked_date: Mapped[datetime] = mapped_column(DateTime(), nullable=True)

    def __repr__(self):
        return f"<WorkflowProgress {self.item_name} (progress_timestamp={self.progress_timestamp}, last_checked_date={self.last_checked_date})>"

    def get_item_name(self) -> str:
        return self.item_name

    def get_progress_timestamp(self) -> datetime | None:
        return self.progress_timestamp

    def get_last_checked_date(self) -> datetime | None:
        return self.last_checked_date

    def update_progress_timestamp(self, progress_timestamp: datetime):
        logger.info(
            f"Updating progress timestamp for {self.item_name} to {progress_timestamp.strftime('%d/%m/%Y %H:%M:%S')}."
        )
        self.progress_timestamp = progress_timestamp

    def update_last_checked_timestamp(self, last_checked_timestamp: datetime):
        logger.info(
            f"Updating last checked timestamp for {self.item_name} to {last_checked_timestamp.strftime('%d/%m/%Y %H:%M:%S')}."
        )
        self.last_checked_date = last_checked_timestamp
