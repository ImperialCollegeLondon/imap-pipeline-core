import logging

import pytest
from wiremock.testing.testcontainer import wiremock_container

from tests.util.miscellaneous import enableLogging  # noqa: F401
from tests.util.wiremock import WireMockManager


@pytest.fixture(scope="function", autouse=False)
def capture_logs(caplog, enableLogging):  # noqa: F811
    """Capture logs for the duration of the test."""
    caplog.set_level(logging.DEBUG)
    yield caplog


@pytest.fixture(scope="session", autouse=False)
def wiremock_manager():
    with wiremock_container(secure=False) as mock_container:
        yield WireMockManager(mock_container)
