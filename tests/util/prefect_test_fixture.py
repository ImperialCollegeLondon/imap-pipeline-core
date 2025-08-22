import pytest
from prefect.testing.utilities import prefect_test_harness

from imap_mag.util import Environment


@pytest.fixture(autouse=False, scope="session")
def prefect_test_fixture():
    # slow startup longer than 30s on GitHub Actions
    with Environment(PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS="60"):
        with prefect_test_harness(server_startup_timeout=60):
            yield
