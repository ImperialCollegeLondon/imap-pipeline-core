import hashlib
import logging
from datetime import datetime
from pathlib import Path

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func

from imap_mag import __version__
from imap_mag.config.AppSettings import AppSettings

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
    content_date: Mapped[datetime] = mapped_column(DateTime(), nullable=True)
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

    def get_file_type_string(self) -> str:
        # convert
        # imap_mag_l1_hsk-status_20251201_v001.csv to imap_mag_l1_hsk-status
        # imap_mag_l2-burst-offsets_20250421_20250421_v000.cdf to imap_mag_l2-burst-offsets
        # imap_mag_l1d_burst-srf_20251207_v001.cdf to imap_mag_l1d_burst-srf
        parts = self.name.rsplit(".", 1)[0].split("_")

        # remove all trailing parts that are date or version
        while parts and (parts[-1].startswith("v") or parts[-1].isdigit()):
            parts.pop()

        return "_".join(parts)

    @classmethod
    def from_file(
        cls,
        file: Path,
        version: int,
        hash: str | None,
        content_date: datetime | None,
        settings: AppSettings,
    ) -> "File":
        if not file.exists():
            raise FileNotFoundError(f"File {file} does not exist.")

        if hash is None or hash == "":
            hash = hashlib.md5(file.read_bytes()).hexdigest()

        size = file.stat().st_size

        try:
            file_with_app_relative_path = file.absolute().relative_to(
                settings.data_store.absolute()
            )
        # match exception by message text "not a subpath"
        except ValueError as e:
            if "is not in the subpath of" in str(e):
                logger.warning(
                    f"File {file} is not within the data store path {settings.data_store}"
                )
                file_with_app_relative_path = file
            else:
                raise

        return cls(
            name=file.name,
            path=file_with_app_relative_path.as_posix(),
            version=version,
            hash=hash,
            size=size,
            content_date=content_date,
            creation_date=datetime.fromtimestamp(file.stat().st_ctime),
            last_modified_date=datetime.fromtimestamp(file.stat().st_mtime),
            software_version=__version__,
        )


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

    def update_last_checked_date(self, last_checked_date: datetime):
        logger.info(
            f"Updating last checked date for {self.item_name} to {last_checked_date.strftime('%d/%m/%Y %H:%M:%S')}."
        )
        self.last_checked_date = last_checked_date
