"""Main module."""

import os
import pathlib
import signal

import sqlalchemy
import typer
from alembic import command, config
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy_utils import create_database, database_exists, drop_database

from .model import Base, File

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
folder = pathlib.Path(__file__).parent.resolve()
alembic_ini = os.path.join(folder, "alembic.ini")
config = config.Config(alembic_ini)

app = typer.Typer()

# enable overriding the database url from the OS env
env_override_url = os.getenv("SQLALCHEMY_URL")
if env_override_url is not None and len(env_override_url) > 0:
    url = config.set_main_option("sqlalchemy.url", env_override_url)

url = config.get_main_option("sqlalchemy.url")
engine = create_engine(url, echo=True)  # echo for dev to outputing SQL


@app.command()
def create_db(with_schema: bool = False, with_data: bool = False):
    print("sql sqlalchemy version: " + sqlalchemy.__version__)

    def timeout_handler(signum, frame):
        raise TimeoutError("Database operation timed out")

    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(30)  # 30 second timeout

    try:
        if not database_exists(engine.url):
            print("Creating db")
            create_database(engine.url)
        else:
            print("Db already exists")
    finally:
        signal.alarm(0)  # Cancel the alarm

    if with_schema:
        print("Creating all tables")
        Base.metadata.create_all(engine)

    if with_data:
        print("Loading some data")
        with Session(engine) as session:
            f1 = File(name="file1.txt", path="/path/to/file1.txt")
            session.add_all([f1])
            session.commit()

    print("Database create complete")


@app.command()
def drop_db():
    if database_exists(engine.url):
        print("Dropping db")
        drop_database(engine.url)
    else:
        print("Skipped - Db does not exist")


@app.command()
def query_db():
    session = Session(engine)

    # stmt = select(File).where(File.name.in_(["file1.txt"]))
    stmt = select(File).where(File.name is not None)

    for user in session.scalars(stmt):
        print(user)


@app.command()
def upgrade_db():
    folder = pathlib.Path(__file__).parent.resolve()
    script_location = "migrations"

    # combine them in OS agnostic way
    script_location = os.path.join(folder, script_location)

    print("Running DB migrations in %r", script_location)

    config.set_main_option("script_location", script_location)
    config.set_main_option("sqlalchemy.url", url)
    command.upgrade(config, "head")


if __name__ == "__main__":
    app()  # pragma: no cover
