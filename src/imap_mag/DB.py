import os

from imap_db.model import File
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


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

    def insert_files(self, files: list[File]):
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
