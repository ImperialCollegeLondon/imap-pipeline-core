import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from testcontainers.postgres import PostgresContainer

from imap_db.model import Base
from imap_mag.db import Database
from imap_mag.util import Environment


@pytest.fixture(
    scope="session",
)
def test_database_container():
    with PostgresContainer(driver="psycopg") as postgres:
        yield postgres


@pytest.fixture(
    scope="session",
)
def test_database_server_engine(test_database_container):
    engine = create_engine(test_database_container.get_connection_url())
    Base.metadata.create_all(engine)

    with Environment(SQLALCHEMY_URL=test_database_container.get_connection_url()):
        yield engine

    engine.dispose()


@pytest.fixture(
    scope="function",
)
def test_database(test_database_server_engine):
    maker = sessionmaker(bind=test_database_server_engine)

    # truncate all tables in the database
    with maker() as session:
        for table in Base.metadata.sorted_tables:
            session.execute(table.delete())
        session.commit()

    yield Database(db_url=test_database_server_engine.url)
