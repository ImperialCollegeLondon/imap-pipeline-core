"""AppSettings against a real Prefect server."""

import pytest
from prefect import flow
from prefect_sqlalchemy import ConnectionComponents, SqlAlchemyConnector
from pydantic import SecretStr

from imap_mag.config.AppSettings import AppSettings
from prefect_server.constants import PREFECT_CONSTANTS
from tests.util.prefect_test_utils import prefect_test_fixture  # noqa: F401


@pytest.fixture
def imap_database_block(prefect_test_fixture):  # noqa: F811
    block = SqlAlchemyConnector(
        connection_info=ConnectionComponents(
            driver="postgresql",
            database="imap",
            host="db.example.com",
            port=5432,
            username="user",
            password=SecretStr("example-password"),
        )
    )
    block.save(PREFECT_CONSTANTS.IMAP_DATABASE_BLOCK_NAME, overwrite=True, _sync=True)
    yield block
    SqlAlchemyConnector.delete(PREFECT_CONSTANTS.IMAP_DATABASE_BLOCK_NAME, _sync=True)


@pytest.mark.imap_database_block
@pytest.mark.asyncio
async def test_create_in_async_flow_reads_db_url_from_imap_database_block(
    imap_database_block, monkeypatch
):
    monkeypatch.delenv("SQLALCHEMY_URL", raising=False)
    monkeypatch.delenv("MAG_DB_URL", raising=False)

    @flow
    async def build_settings() -> str | None:
        return AppSettings.create(is_prefect=True).db_url

    db_url = await build_settings()

    assert (
        db_url == "postgresql+psycopg://user:example-password@db.example.com:5432/imap"
    )
