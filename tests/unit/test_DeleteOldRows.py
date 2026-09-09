"""Unit tests for imap_mag.db.DeleteOldRows."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from psycopg.errors import InsufficientPrivilege, UndefinedTable
from sqlalchemy.exc import ProgrammingError

from imap_mag.db.DeleteOldRows import delete_old_rows
from imap_mag.util import DatetimeProvider


def _make_mock_engine(scalar_return=None, rowcount=0):
    mock_connection = MagicMock()
    mock_connection.execute.return_value.scalar_one.return_value = scalar_return
    mock_connection.execute.return_value.rowcount = rowcount

    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_connection
    return mock_engine, mock_connection


class TestDeleteOldRowsValidation:
    def test_raises_for_invalid_table_name(self) -> None:
        with pytest.raises(ValueError, match="Invalid table name"):
            delete_old_rows(
                "postgresql://test",
                "bad; drop table users",
                "id",
                7,
                dry_run=True,
            )

    def test_raises_for_invalid_column_name(self) -> None:
        with pytest.raises(ValueError, match="Invalid column name"):
            delete_old_rows(
                "postgresql://test",
                "ace_mag_noaa",
                "bad column",
                7,
                dry_run=True,
            )


class TestDeleteOldRowsDelete:
    def test_deletes_rows_and_returns_rowcount(self) -> None:
        mock_engine, mock_connection = _make_mock_engine(rowcount=3)

        with patch("imap_mag.db.DeleteOldRows.create_engine", return_value=mock_engine):
            affected = delete_old_rows(
                "postgresql://test",
                "ace_mag_noaa",
                "id",
                7,
                dry_run=False,
                datetime_provider=DatetimeProvider(fixed_now=datetime(2026, 7, 21)),
            )

        assert affected == 3
        statement, params = mock_connection.execute.call_args[0]
        assert "DELETE FROM" in str(statement)
        assert "ace_mag_noaa" in str(statement)
        assert params["cutoff"] == datetime(2026, 7, 14)

    def test_converts_postgresql_scheme_to_use_psycopg_driver(self) -> None:
        mock_engine, _ = _make_mock_engine(rowcount=0)

        with patch(
            "imap_mag.db.DeleteOldRows.create_engine", return_value=mock_engine
        ) as mock_create_engine:
            delete_old_rows(
                "postgresql://user:pass@host/db", "ace_mag_noaa", "id", 7, dry_run=False
            )

        mock_create_engine.assert_called_once_with(
            "postgresql+psycopg://user:pass@host/db"
        )


class TestDeleteOldRowsDryRun:
    def test_counts_rows_without_deleting(self) -> None:
        mock_engine, mock_connection = _make_mock_engine(scalar_return=5)

        with patch("imap_mag.db.DeleteOldRows.create_engine", return_value=mock_engine):
            affected = delete_old_rows(
                "postgresql://test", "ace_mag_noaa", "id", 7, dry_run=True
            )

        assert affected == 5
        statement = mock_connection.execute.call_args[0][0]
        assert "SELECT COUNT" in str(statement)
        assert "DELETE" not in str(statement)


class TestDeleteOldRowsMissingTable:
    def _make_engine_raising(self, orig_exception):
        mock_connection = MagicMock()
        mock_connection.execute.side_effect = ProgrammingError(
            "statement", {}, orig_exception
        )

        mock_engine = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_connection
        return mock_engine

    def test_returns_zero_when_table_does_not_exist_on_delete(self) -> None:
        mock_engine = self._make_engine_raising(
            UndefinedTable('relation "ace_mag_noaa" does not exist')
        )

        with patch("imap_mag.db.DeleteOldRows.create_engine", return_value=mock_engine):
            affected = delete_old_rows(
                "postgresql://test", "ace_mag_noaa", "id", 7, dry_run=False
            )

        assert affected == 0

    def test_returns_zero_when_table_does_not_exist_on_dry_run(self) -> None:
        mock_engine = self._make_engine_raising(
            UndefinedTable('relation "ace_mag_noaa" does not exist')
        )

        with patch("imap_mag.db.DeleteOldRows.create_engine", return_value=mock_engine):
            affected = delete_old_rows(
                "postgresql://test", "ace_mag_noaa", "id", 7, dry_run=True
            )

        assert affected == 0

    def test_reraises_other_programming_errors(self) -> None:
        mock_engine = self._make_engine_raising(
            InsufficientPrivilege("permission denied")
        )

        with (
            patch("imap_mag.db.DeleteOldRows.create_engine", return_value=mock_engine),
            pytest.raises(ProgrammingError),
        ):
            delete_old_rows("postgresql://test", "ace_mag_noaa", "id", 7, dry_run=False)
