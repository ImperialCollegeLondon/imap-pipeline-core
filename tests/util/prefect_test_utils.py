from unittest import mock

import pytest
from prefect.testing.utilities import prefect_test_harness

from imap_mag.util import Environment


@pytest.fixture(autouse=False, scope="session")
def prefect_test_fixture():
    # slow startup longer than 30s on GitHub Actions
    with Environment(PREFECT_SERVER_EPHEMERAL_STARTUP_TIMEOUT_SECONDS="60"):
        with prefect_test_harness(server_startup_timeout=60):
            yield


@pytest.fixture
def mock_teams_webhook_block(mocker) -> mock.Mock:
    mock_block = mock.AsyncMock()
    mock_block.notify = mock.AsyncMock()
    mock_block.notify_type = "info"
    mock_aload = mock.AsyncMock(return_value=mock_block)

    mocker.patch(
        "prefect.blocks.notifications.MicrosoftTeamsWebhook.aload",
        new=mock_aload,
    )

    return mock_block
