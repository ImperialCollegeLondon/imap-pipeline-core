import pytest
from wiremock.testing.testcontainer import wiremock_container

from tests.util.wiremock import WireMockManager


@pytest.fixture(scope="session", autouse=False)
def wiremock_manager():
    with wiremock_container(secure=False) as mock_container:
        yield WireMockManager(mock_container)
