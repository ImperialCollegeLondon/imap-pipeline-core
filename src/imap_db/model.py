from datetime import datetime

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class File(Base):
    __tablename__ = "files"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128))
    path: Mapped[str] = mapped_column(String(256), unique=True)
    version: Mapped[int] = mapped_column(Integer())
    hash: Mapped[str] = mapped_column(String(64))
    date: Mapped[datetime] = mapped_column(DateTime())
    software_version: Mapped[str] = mapped_column(String(16))

    def __repr__(self) -> str:
        return f"<File {self.id} (name={self.name}, path={self.path})>"


class DownloadProgress(Base):
    __tablename__ = "download_progress"

    item_name: Mapped[str] = mapped_column(String(128), primary_key=True, unique=True)
    progress_timestamp: Mapped[datetime] = mapped_column(DateTime(), nullable=True)
    last_checked_date: Mapped[datetime] = mapped_column(DateTime(), nullable=True)

    def __repr__(self):
        return f"<DownloadProgress {self.item_name} (progress_timestamp={self.progress_timestamp}, last_checked_date={self.last_checked_date})>"
