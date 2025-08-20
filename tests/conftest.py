import logging

import pytest
from wiremock.testing.testcontainer import wiremock_container

from tests.util.miscellaneous import enableLogging  # noqa: F401
from tests.util.wiremock import WireMockManager

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
logging.getLogger("sqlalchemy.engine").setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.fixture(scope="function", autouse=False)
def capture_cli_logs(caplog, enableLogging):  # noqa: F811
    """Capture logs for the duration of the test."""
    caplog.set_level(logging.DEBUG)
    yield caplog


@pytest.fixture(scope="session", autouse=False)
def wiremock_manager():
    with wiremock_container(secure=False) as mock_container:
        yield WireMockManager(mock_container)
