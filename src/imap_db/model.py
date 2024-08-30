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
