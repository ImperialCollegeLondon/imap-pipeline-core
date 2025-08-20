import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from testcontainers.postgres import PostgresContainer

from imap_db.model import Base
from imap_mag.db import Database
from imap_mag.util import Environment


@pytest.fixture(scope="function")
def test_database():
    with PostgresContainer(driver="psycopg") as postgres:
        engine = create_engine(postgres.get_connection_url())
        Base.metadata.create_all(engine)

        Session = sessionmaker(bind=engine)
        session = Session()

        with Environment(SQLALCHEMY_URL=postgres.get_connection_url()):
            yield Database(db_url=postgres.get_connection_url())

        session.close()
        engine.dispose()
