"""Unit tests for Database class that do not require a real database connection."""

from datetime import datetime
from unittest.mock import patch

import pytest

from imap_db.model import Base, File
from imap_mag.db.Database import Database, update_database_with_progress


def _make_file(name, path, hash_val, *, deletion_date=None, last_modified_date=None):
    """Build a minimal valid File object for testing."""
    kwargs = dict(
        name=name,
        path=path,
        descriptor="test-descriptor",
        version=1,
        hash=hash_val,
        size=100,
        software_version="1.0",
    )
    if deletion_date is not None:
        kwargs["deletion_date"] = deletion_date
    if last_modified_date is not None:
        kwargs["last_modified_date"] = last_modified_date
    return File(**kwargs)


class TestDatabaseFnmatchToLike:
    def test_converts_star_to_percent(self):
        assert Database._fnmatch_to_like("hk/mag/l1/*") == "hk/mag/l1/%"

    def test_converts_question_mark_to_underscore(self):
        assert Database._fnmatch_to_like("file????") == "file____"

    def test_escapes_existing_percent(self):
        result = Database._fnmatch_to_like("file%name")
        assert r"\%" in result

    def test_escapes_existing_underscore(self):
        result = Database._fnmatch_to_like("file_name")
        assert r"\_" in result

    def test_double_star_becomes_percent(self):
        result = Database._fnmatch_to_like("**/*.csv")
        assert "%" in result

    def test_complex_pattern(self):
        result = Database._fnmatch_to_like("*hk/mag/l1/*_v*.cdf")
        assert result == "%hk/mag/l1/%\\_v%.cdf"


class TestDatabaseGetEnvironmentUrl:
    def test_returns_none_when_env_var_not_set(self):
        with patch.dict("os.environ", {}, clear=True):
            result = Database.get_environment_url()
        assert result is None

    def test_returns_url_from_env_var(self):
        with patch.dict("os.environ", {"SQLALCHEMY_URL": "sqlite:///test.db"}):
            result = Database.get_environment_url()
        assert result == "sqlite:///test.db"


class TestDatabaseInit:
    def test_raises_when_no_url_provided(self):
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(ValueError, match="No database URL"):
                Database()

    def test_uses_env_var_url_when_no_explicit_url(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        with patch.dict("os.environ", {"SQLALCHEMY_URL": f"sqlite:///{db_path}"}):
            db = Database()
        assert db.engine is not None

    def test_uses_explicit_url_when_provided(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        db = Database(db_url=f"sqlite:///{db_path}")
        assert db.engine is not None


class TestUpdateDatabaseWithProgress:
    def test_updates_last_checked_timestamp(self, tmp_path):
        db = Database(db_url=f"sqlite:///{tmp_path}/test.db")
        Base.metadata.create_all(db.engine)

        checked = datetime(2025, 6, 1, 12, 0, 0)
        latest = datetime(2025, 6, 1, 11, 0, 0)

        update_database_with_progress("TEST_ITEM", db, checked, latest)

        progress = db.get_workflow_progress("TEST_ITEM")
        assert progress.get_last_checked_date() == checked
        assert progress.progress_timestamp == latest

    def test_does_not_update_progress_when_latest_is_older(self, tmp_path):
        db = Database(db_url=f"sqlite:///{tmp_path}/test.db")
        Base.metadata.create_all(db.engine)

        newer_progress = datetime(2025, 6, 1, 12, 0, 0)
        checked = datetime(2025, 6, 2)

        wp = db.get_workflow_progress("TEST_ITEM2")
        wp.update_progress_timestamp(newer_progress)
        db.save(wp)

        update_database_with_progress("TEST_ITEM2", db, checked, datetime(2025, 5, 1))

        progress = db.get_workflow_progress("TEST_ITEM2")
        assert progress.progress_timestamp == newer_progress

    def test_handles_none_latest_timestamp(self, tmp_path):
        db = Database(db_url=f"sqlite:///{tmp_path}/test.db")
        Base.metadata.create_all(db.engine)

        checked = datetime(2025, 6, 1)
        update_database_with_progress("TEST_ITEM3", db, checked, None)

        progress = db.get_workflow_progress("TEST_ITEM3")
        assert progress.get_last_checked_date() == checked


class TestDatabaseOperations:
    @pytest.fixture
    def sqlite_db(self, tmp_path):
        db = Database(db_url=f"sqlite:///{tmp_path}/test.db")
        Base.metadata.create_all(db.engine)
        return db

    def test_insert_and_get_file(self, sqlite_db):
        f = _make_file("test.cdf", "science/mag/l2/test.cdf", "abc123")
        sqlite_db.upsert_file(f)

        files = sqlite_db.get_files(name="test.cdf")
        assert len(files) == 1
        assert files[0].name == "test.cdf"

    def test_insert_duplicate_file_same_hash_skips(self, sqlite_db):
        f1 = _make_file("dup.cdf", "science/dup.cdf", "same_hash")
        sqlite_db.upsert_file(f1)
        f2 = _make_file("dup.cdf", "science/dup.cdf", "same_hash")
        sqlite_db.upsert_file(f2)

        files = sqlite_db.get_files(name="dup.cdf")
        assert len(files) == 1

    def test_insert_duplicate_file_different_hash_updates(self, sqlite_db):
        f1 = _make_file("upd.cdf", "science/upd.cdf", "hash_v1")
        sqlite_db.upsert_file(f1)

        f2 = _make_file("upd.cdf", "science/upd.cdf", "hash_v2")
        sqlite_db.upsert_file(f2)

        files = sqlite_db.get_files(name="upd.cdf")
        assert len(files) == 1
        assert files[0].hash == "hash_v2"

    def test_get_files_by_path(self, sqlite_db):
        f1 = _make_file("f1.cdf", "science/mag/l2/f1.cdf", "h1")
        f2 = _make_file("f2.cdf", "hk/mag/l1/f2.cdf", "h2")
        sqlite_db.upsert_files([f1, f2])

        results = sqlite_db.get_files_by_path("science/")
        assert len(results) == 1
        assert results[0].name == "f1.cdf"

    def test_get_all_active_files_excludes_deleted(self, sqlite_db):
        f1 = _make_file("active.cdf", "science/active.cdf", "h1")
        f2 = _make_file(
            "deleted.cdf",
            "science/deleted.cdf",
            "h2",
            deletion_date=datetime(2025, 1, 1),
        )
        sqlite_db.upsert_files([f1, f2])

        results = sqlite_db.get_all_active_files()
        names = [f.name for f in results]
        assert "active.cdf" in names
        assert "deleted.cdf" not in names

    def test_get_files_since_returns_newer_files(self, sqlite_db):
        f = _make_file(
            "new.cdf", "science/new.cdf", "h1", last_modified_date=datetime(2025, 6, 2)
        )
        sqlite_db.upsert_file(f)

        results = sqlite_db.get_files_since(datetime(2025, 6, 1))
        assert any(f.name == "new.cdf" for f in results)

    def test_get_files_deleted_since_returns_deleted_files(self, sqlite_db):
        f = _make_file(
            "gone.cdf", "science/gone.cdf", "h_gone", deletion_date=datetime(2025, 6, 2)
        )
        sqlite_db.upsert_file(f)

        results = sqlite_db.get_files_deleted_since(datetime(2025, 6, 1))
        assert any(f.name == "gone.cdf" for f in results)

    def test_get_workflow_progress_creates_new_when_not_found(self, sqlite_db):
        progress = sqlite_db.get_workflow_progress("NEW_ITEM")
        assert progress is not None
        assert progress.item_name == "NEW_ITEM"

    def test_save_and_get_workflow_progress(self, sqlite_db):
        wp = sqlite_db.get_workflow_progress("SAVE_TEST")
        wp.update_last_checked_timestamp(datetime(2025, 6, 1))
        sqlite_db.save(wp)

        retrieved = sqlite_db.get_workflow_progress("SAVE_TEST")
        assert retrieved.get_last_checked_date() == datetime(2025, 6, 1)

    def test_get_all_workflow_progress_returns_saved_items(self, sqlite_db):
        wp = sqlite_db.get_workflow_progress("WP1")
        wp.update_last_checked_timestamp(datetime(2025, 6, 1))
        sqlite_db.save(wp)

        results = sqlite_db.get_all_workflow_progress()
        assert any(w.item_name == "WP1" for w in results)

    def test_get_files_by_path_pattern_returns_matching(self, sqlite_db):
        f = _make_file("match.cdf", "science/mag/l2/match.cdf", "hm")
        sqlite_db.upsert_file(f)

        results = sqlite_db.get_files_by_path_pattern("science/mag/%")
        assert any(f.name == "match.cdf" for f in results)

    def test_get_active_files_matching_patterns_uses_fnmatch(self, sqlite_db):
        f = _make_file("file.cdf", "science/mag/l2/file.cdf", "hf")
        sqlite_db.upsert_file(f)

        results = sqlite_db.get_active_files_matching_patterns(["science/mag/*"])
        assert any(f.name == "file.cdf" for f in results)

    def test_get_active_files_matching_patterns_empty_list_returns_empty(
        self, sqlite_db
    ):
        results = sqlite_db.get_active_files_matching_patterns([])
        assert results == []

    def test_get_files_since_with_limit(self, sqlite_db):
        files = [
            _make_file(
                f"file{i}.cdf",
                f"science/file{i}.cdf",
                f"h{i}",
                last_modified_date=datetime(2025, 6, i + 2),
            )
            for i in range(5)
        ]
        sqlite_db.upsert_files(files)

        results = sqlite_db.get_files_since(datetime(2025, 6, 1), how_many=2)
        assert len(results) <= 2
