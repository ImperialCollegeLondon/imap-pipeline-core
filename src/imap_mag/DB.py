import os
from pathlib import Path

from imap_db.model import File
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.imap_db.model import File
from src.imap_mag import __version__
from src.imap_mag.outputManager import IMetadataProvider, IOutputManager


class DB:
    def __init__(self, db_url=None):
        env_url = os.getenv("SQLALCHEMY_URL")
        if db_url is None and env_url is not None:
            db_url = env_url

        if db_url is None:
            raise ValueError(
                "No database URL provided. Consider setting SQLALCHEMY_URL environment variable."
            )

        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)

    def insert_file(self, file: File) -> None:
        self.insert_files([file])

    def insert_files(self, files: list[File]) -> None:
        session = self.Session()
        try:
            for file in files:
                # check file does not already exist
                existing_file = (
                    session.query(File)
                    .filter_by(name=file.name, path=file.path)
                    .first()
                )
                if existing_file is not None:
                    continue

                session.add(file)

            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()


class DatabaseOutputManager(IOutputManager):
    def __init__(self, output_manager: IOutputManager, db: DB = DB()):
        self.output_manager = output_manager
        self.db = db

    def add_file(
        self, original_file: Path, metadata_provider: IMetadataProvider
    ) -> tuple[Path, IMetadataProvider]:
        (destination_file, metadata_provider) = self.output_manager.add_file(
            original_file, metadata_provider
        )

        self.db.insert_file(
            File(
                name=destination_file.name,
                path=destination_file.absolute().as_posix(),
                version=metadata_provider.version,
                date=metadata_provider.date,
                software_version=__version__,
            )
        )

        return (destination_file, metadata_provider)
