"""Tests for Prefect server constants."""

from prefect_server.constants import PREFECT_CONSTANTS


class TestPrefectConstants:
    def test_flow_names_are_defined(self):
        assert PREFECT_CONSTANTS.FLOW_NAMES.POLL_IALIRT == "poll-ialirt"
        assert PREFECT_CONSTANTS.FLOW_NAMES.POLL_HK == "poll-hk"
        assert PREFECT_CONSTANTS.FLOW_NAMES.POLL_SCIENCE == "poll-science"

    def test_queue_names_are_defined(self):
        assert PREFECT_CONSTANTS.QUEUES.HIGH_PRIORITY is not None
        assert PREFECT_CONSTANTS.QUEUES.DEFAULT is not None
        assert PREFECT_CONSTANTS.QUEUES.LOW_BIG is not None

    def test_block_names_are_defined(self):
        assert PREFECT_CONSTANTS.IMAP_DATABASE_BLOCK_NAME is not None
        assert PREFECT_CONSTANTS.IMAP_WEBHOOK_BLOCK_NAME is not None

    def test_env_var_names_are_defined(self):
        assert PREFECT_CONSTANTS.ENV_VAR_NAMES.SQLALCHEMY_URL == "SQLALCHEMY_URL"
        assert (
            PREFECT_CONSTANTS.ENV_VAR_NAMES.POLL_IALIRT_CRON == "IMAP_CRON_POLL_IALIRT"
        )
