"""Unit tests for DeleteOldDatabaseRowsConfig."""

from imap_mag.config.DeleteOldDatabaseRowsConfig import DeleteOldDatabaseRowsConfig


class TestDeleteOldDatabaseRowsConfigDefaults:
    def test_defaults(self) -> None:
        config = DeleteOldDatabaseRowsConfig()

        assert config.dry_run is True
        assert config.database_url_env_var_or_block_name == "DATABASE_URL"

    def test_can_override_defaults(self) -> None:
        config = DeleteOldDatabaseRowsConfig(
            dry_run=False,
            database_url_env_var_or_block_name="imap-database",
        )

        assert config.dry_run is False
        assert config.database_url_env_var_or_block_name == "imap-database"
