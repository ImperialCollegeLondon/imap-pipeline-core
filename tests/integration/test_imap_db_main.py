"""Tests for imap_db/main.py database CLI commands."""

from unittest.mock import MagicMock, patch

from imap_db.main import create_db, drop_db, query_db, upgrade_db


class TestCreateDb:
    def test_create_db_when_database_does_not_exist(self):

        with (
            patch("imap_db.main.database_exists", return_value=False),
            patch("imap_db.main.create_database") as mock_create,
        ):
            create_db()

        mock_create.assert_called_once()

    def test_create_db_skips_when_database_exists(self):

        with (
            patch("imap_db.main.database_exists", return_value=True),
            patch("imap_db.main.create_database") as mock_create,
        ):
            create_db()

        mock_create.assert_not_called()

    def test_create_db_with_schema_creates_tables(self):

        mock_engine = MagicMock()

        with (
            patch("imap_db.main.database_exists", return_value=True),
            patch("imap_db.main.engine", mock_engine),
            patch("imap_db.main.Base") as mock_base,
        ):
            create_db(with_schema=True)

        mock_base.metadata.create_all.assert_called_once_with(mock_engine)

    def test_create_db_with_data_adds_sample_file(self):

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session_class = MagicMock(return_value=mock_session)

        with (
            patch("imap_db.main.database_exists", return_value=True),
            patch("imap_db.main.Session", mock_session_class),
        ):
            create_db(with_data=True)

        mock_session.add_all.assert_called_once()
        mock_session.commit.assert_called_once()


class TestDropDb:
    def test_drop_db_when_database_exists(self):

        with (
            patch("imap_db.main.database_exists", return_value=True),
            patch("imap_db.main.drop_database") as mock_drop,
        ):
            drop_db()

        mock_drop.assert_called_once()

    def test_drop_db_skips_when_database_does_not_exist(self):

        with (
            patch("imap_db.main.database_exists", return_value=False),
            patch("imap_db.main.drop_database") as mock_drop,
        ):
            drop_db()

        mock_drop.assert_not_called()


class TestQueryDb:
    def test_query_db_runs_select_statement(self):

        mock_session = MagicMock()
        mock_session.scalars.return_value = []
        mock_session_class = MagicMock(return_value=mock_session)

        with patch("imap_db.main.Session", mock_session_class):
            query_db()

        mock_session.scalars.assert_called_once()


class TestUpgradeDb:
    def test_upgrade_db_runs_alembic_migration(self):

        with patch("imap_db.main.command") as mock_command:
            upgrade_db()

        mock_command.upgrade.assert_called_once()
        call_args = mock_command.upgrade.call_args
        assert call_args[0][1] == "head"
