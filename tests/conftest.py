import logging

import pytest
from wiremock.testing.testcontainer import wiremock_container

from tests.util.miscellaneous import enableLogging  # noqa: F401
from tests.util.wiremock import WireMockManager


@pytest.fixture(scope="function", autouse=False)
def capture_cli_logs(caplog, enableLogging):  # noqa: F811
    """Capture logs for the duration of the test."""
    caplog.set_level(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger="imap_db")
    caplog.set_level(logging.DEBUG, logger="imap_mag")
    caplog.set_level(logging.DEBUG, logger="mag_toolkit")
    caplog.set_level(logging.DEBUG, logger="prefect_server")
    yield caplog


@pytest.fixture(scope="session", autouse=False)
def wiremock_manager():
    with wiremock_container(secure=False) as mock_container:
        yield WireMockManager(mock_container)
