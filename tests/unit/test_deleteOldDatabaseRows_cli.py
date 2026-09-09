"""Unit tests for the delete-old-database-rows CLI command."""

from unittest.mock import MagicMock, patch

from imap_mag.cli.deleteOldDatabaseRows import delete_old_database_rows

_PATCH_TARGET = "imap_mag.cli.deleteOldDatabaseRows"


def _make_mock_settings(dry_run=False):
    mock_settings = MagicMock()
    mock_settings.delete_old_database_rows.dry_run = dry_run
    mock_settings.delete_old_database_rows.database_url_env_var_or_block_name = (
        "imap-database"
    )
    return mock_settings


class TestDeleteOldDatabaseRowsCli:
    def test_deletes_rows_for_given_table(self) -> None:
        mock_settings = _make_mock_settings()

        with (
            patch(f"{_PATCH_TARGET}.AppSettings", return_value=mock_settings),
            patch(f"{_PATCH_TARGET}.initialiseLoggingForCommand"),
            patch(
                f"{_PATCH_TARGET}._get_database_connectionstring",
                return_value="postgresql://localhost/test",
            ),
            patch(f"{_PATCH_TARGET}.delete_old_rows", return_value=1) as mock_delete,
        ):
            delete_old_database_rows(table="ace_mag_noaa", older_than_days=7)

        mock_delete.assert_called_once()
        call_args = mock_delete.call_args.args
        assert call_args[:5] == (
            "postgresql://localhost/test",
            "ace_mag_noaa",
            "id",
            7,
            False,
        )

    def test_dry_run_argument_overrides_config_default(self) -> None:
        mock_settings = _make_mock_settings(dry_run=False)

        with (
            patch(f"{_PATCH_TARGET}.AppSettings", return_value=mock_settings),
            patch(f"{_PATCH_TARGET}.initialiseLoggingForCommand"),
            patch(
                f"{_PATCH_TARGET}._get_database_connectionstring",
                return_value="postgresql://localhost/test",
            ),
            patch(f"{_PATCH_TARGET}.delete_old_rows", return_value=0) as mock_delete,
        ):
            delete_old_database_rows(table="ace_mag_noaa", dry_run=True)

        assert mock_delete.call_args.args[4] is True  # dry_run positional argument

    def test_uses_config_dry_run_when_argument_not_provided(self) -> None:
        mock_settings = _make_mock_settings(dry_run=True)

        with (
            patch(f"{_PATCH_TARGET}.AppSettings", return_value=mock_settings),
            patch(f"{_PATCH_TARGET}.initialiseLoggingForCommand"),
            patch(
                f"{_PATCH_TARGET}._get_database_connectionstring",
                return_value="postgresql://localhost/test",
            ),
            patch(f"{_PATCH_TARGET}.delete_old_rows", return_value=0) as mock_delete,
        ):
            delete_old_database_rows(table="ace_mag_noaa")

        assert mock_delete.call_args.args[4] is True
