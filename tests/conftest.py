import logging
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
from wiremock.testing.testcontainer import wiremock_container

from imap_mag.util.Environment import Environment
from tests.util.WireMockManager import WireMockManager

# quieten some loggers for dependencies when run in tests with debug
print("\n[conftest.py] Setting loggers to INFO level for dependencies...\n")
logging.getLogger("paramiko").setLevel(logging.INFO)
logging.getLogger("urllib3").setLevel(logging.INFO)
logging.getLogger("httpcore.connection").setLevel(logging.INFO)
logging.getLogger("httpcore.http11").setLevel(logging.INFO)
logging.getLogger("asyncio:selector_events.py").setLevel(logging.INFO)
logging.getLogger("graphviz").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.INFO)
logging.getLogger("testcontainers").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
logger = logging.getLogger(__name__)

pytest_plugins = (
    "tests.util.database",
    "tests.util.miscellaneous",
)


@pytest.fixture(scope="function", autouse=False)
def capture_cli_logs(caplog, enableLogging):
    """Capture logs for the duration of the test."""
    caplog.set_level(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger="imap_db")
    caplog.set_level(logging.DEBUG, logger="imap_mag")
    caplog.set_level(logging.DEBUG, logger="mag_toolkit")
    caplog.set_level(logging.DEBUG, logger="prefect_server")
    yield caplog


@pytest.fixture(
    scope="session",
    autouse=False,
    params=[pytest.param("", marks=pytest.mark.xdist_group("sdc-api"))],
)
def wiremock_manager():
    with wiremock_container(secure=False) as mock_container:
        yield WireMockManager(mock_container)


@pytest.fixture(autouse=False)
def temp_file_path() -> Generator[Path, None, None]:
    """Fixture to create a temporary file for testing."""

    with tempfile.NamedTemporaryFile() as temp_file:
        # Write some content to the temporary file
        temp_file.write(b"test-input-file")
        temp_file.flush()  # Ensure the content is written to disk
        yield Path(temp_file.name)


@pytest.fixture(autouse=False)
def temp_folder_path() -> Generator[Path, None, None]:
    """Fixture to create a temporary folder for testing."""

    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture(autouse=False, scope="function")
def dynamic_work_folder(temp_folder_path: Path) -> Generator[Path, None, None]:
    """
    Fixture to create a dynamic (temp) work folder for the test and set the ENV var MAG_WORK_FOLDER so AppSettings picks it up
    """
    # override AppSettings.work_folder with a temp folder and not ".work"
    with Environment(MAG_WORK_FOLDER=str(temp_folder_path)):
        yield temp_folder_path


@pytest.fixture(autouse=False, scope="function")
def clean_datastore(temp_folder_path: Path):
    """
    Fixture to create an empty (temp) data store folder for the test and set the ENV var MAG_DATA_STORE so AppSettings picks it up
    """
    # override AppSettings.data_store with a temp folder and not ".work"
    with Environment(MAG_DATA_STORE=str(temp_folder_path)):
        yield temp_folder_path
