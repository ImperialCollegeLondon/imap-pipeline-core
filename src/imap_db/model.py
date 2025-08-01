import logging
from datetime import datetime
from pathlib import Path

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func

from imap_mag import __version__

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class File(Base):
    __tablename__ = "files"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128))
    path: Mapped[str] = mapped_column(String(256), unique=True)
    version: Mapped[int] = mapped_column(Integer())
    hash: Mapped[str] = mapped_column(String(64))
    size: Mapped[int] = mapped_column(Integer())
    content_date: Mapped[datetime] = mapped_column(DateTime())
    creation_date: Mapped[datetime] = mapped_column(
        DateTime(), server_default=func.now()
    )
    last_modified_date: Mapped[datetime] = mapped_column(
        DateTime(), onupdate=func.now()
    )
    deletion_date: Mapped[datetime | None] = mapped_column(DateTime(), nullable=True)
    software_version: Mapped[str] = mapped_column(String(16))

    def __repr__(self) -> str:
        return f"<File {self.id} (name={self.name}, path={self.path})>"

    @classmethod
    def from_file(
        cls,
        file: Path,
        version: int,
        original_hash: str,
        content_date: datetime,
    ) -> "File":
        return cls(
            name=file.name,
            path=file.absolute().as_posix(),
            version=version,
            hash=original_hash,
            size=file.stat().st_size,
            content_date=content_date,
            creation_date=datetime.fromtimestamp(file.stat().st_ctime),
            last_modified_date=datetime.fromtimestamp(file.stat().st_mtime),
            software_version=__version__,
        )


class DownloadProgress(Base):
    __tablename__ = "download_progress"

    item_name: Mapped[str] = mapped_column(String(32), primary_key=True, unique=True)
    progress_timestamp: Mapped[datetime] = mapped_column(DateTime(), nullable=True)
    last_checked_date: Mapped[datetime] = mapped_column(DateTime(), nullable=True)

    def __repr__(self):
        return f"<DownloadProgress {self.item_name} (progress_timestamp={self.progress_timestamp}, last_checked_date={self.last_checked_date})>"

    def get_item_name(self) -> str:
        return self.item_name

    def get_progress_timestamp(self) -> datetime:
        return self.progress_timestamp

    def get_last_checked_date(self) -> datetime:
        return self.last_checked_date

    def record_successful_download(self, progress_timestamp: datetime):
        logger.info(
            f"Updating progress timestamp for {self.item_name} to {progress_timestamp.strftime('%d/%m/%Y %H:%M:%S')}."
        )
        self.progress_timestamp = progress_timestamp

    def record_checked_download(self, last_checked_date: datetime):
        logger.info(
            f"Updating last checked date for {self.item_name} to {last_checked_date.strftime('%d/%m/%Y %H:%M:%S')}."
        )
        self.last_checked_date = last_checked_date
