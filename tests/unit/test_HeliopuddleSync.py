"""Unit tests for mirroring the files table into heliopuddle."""

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from heliopuddle.testing import Call, FakeHelioPuddleClient

from imap_db.model import Base, File
from imap_mag.config.HeliopuddleConfig import HeliopuddleConfig
from imap_mag.db.Database import Database
from imap_mag.db.HeliopuddleSync import (
    PROGRESS_ITEM_NAME,
    FileSnapshot,
    HeliopuddleSync,
    HeliopuddleSyncError,
    SyncAction,
    run_sync,
)

CONTENT_DATE = datetime(2026, 3, 4)


def _config(**overrides) -> HeliopuddleConfig:
    values = dict(enabled=True, url="http://hp.example/api/v1", project="imap")
    values.update(overrides)
    return HeliopuddleConfig(**values)


def _write(store: Path, path: str, data: bytes) -> tuple[str, int]:
    target = store / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    return hashlib.md5(data).hexdigest(), len(data)


def _file(path: str, size: int = 4, **overrides) -> File:
    values = dict(
        name=path.rsplit("/", 1)[-1],
        path=path,
        descriptor="imap_mag_l1_test",
        version=1,
        version_major=1,
        hash="imap-identity-hash",
        size=size,
        content_date=CONTENT_DATE,
        software_version="1.0.0",
        file_meta={"source": "unit"},
    )
    values.update(overrides)
    return File(**values)


@pytest.fixture
def store(tmp_path: Path) -> Path:
    path = tmp_path / "datastore"
    path.mkdir()
    return path


@pytest.fixture
def hp(store: Path) -> FakeHelioPuddleClient:
    return FakeHelioPuddleClient(project="imap", store=store)


@pytest.fixture
def sync(store: Path, hp: FakeHelioPuddleClient) -> HeliopuddleSync:
    return HeliopuddleSync(_config(), store, client=hp, sleep=lambda _: None)  # type: ignore[arg-type]


@pytest.fixture
def db(tmp_path: Path, sync: HeliopuddleSync) -> Database:
    database = Database(db_url=f"sqlite:///{tmp_path}/test.db", heliopuddle_sync=sync)
    Base.metadata.create_all(database.engine)
    return database


# -- config ------------------------------------------------------------------------


class TestHeliopuddleConfig:
    def test_disabled_by_default_and_needs_nothing(self):
        config = HeliopuddleConfig()
        assert not config.enabled
        assert config.shared_datastore
        assert config.fail_on_error

    def test_enabled_requires_url_and_project(self):
        with pytest.raises(ValueError, match="url, project"):
            HeliopuddleConfig(enabled=True)

    def test_durations_are_human_readable(self):
        config = _config(request_timeout="90s", incremental_overlap="2h")
        assert config.request_timeout == timedelta(seconds=90)
        assert config.incremental_overlap == timedelta(hours=2)

    def test_app_settings_reads_the_section_and_env_credentials(self, monkeypatch):
        from imap_mag.config.AppSettings import AppSettings

        monkeypatch.setenv("MAG_HELIOPUDDLE_ENABLED", "true")
        monkeypatch.setenv("MAG_HELIOPUDDLE_URL", "http://hp.example/api/v1")
        monkeypatch.setenv("MAG_HELIOPUDDLE_PASSWORD", "secret")
        monkeypatch.setenv("MAG_HELIOPUDDLE_CLIENT_ID", "an-id")
        monkeypatch.setenv("MAG_HELIOPUDDLE_CLIENT_SECRET", "a-secret")
        settings = AppSettings()  # type: ignore

        assert settings.heliopuddle.enabled
        assert settings.heliopuddle.url == "http://hp.example/api/v1"
        assert settings.heliopuddle.password == "secret"
        assert settings.heliopuddle.client_id == "an-id"
        assert settings.heliopuddle.client_secret == "a-secret"
        assert settings.heliopuddle.project == "imap-mag"
        assert HeliopuddleSync.from_settings(settings) is not None

    def test_the_shipped_config_leaves_it_disabled(self):
        from imap_mag.config.AppSettings import AppSettings

        settings = AppSettings()  # type: ignore
        assert not settings.heliopuddle.enabled
        assert HeliopuddleSync.from_settings(settings) is None


# -- one file ------------------------------------------------------------------------


class TestSyncFile:
    def test_new_file_is_registered_with_its_md5_metadata_and_content_date(
        self, sync, hp, store
    ):
        digest, size = _write(store, "hk/a_v001.csv", b"data")
        snapshot = FileSnapshot.from_file(_file("hk/a_v001.csv", id=7))

        assert sync.sync_file(snapshot) == SyncAction.ADDED

        record = hp.record_at("hk/a_v001.csv")
        assert record.hash == digest  # md5 of the content, not the IMAP identity hash
        assert record.size == size
        assert record.content_date == CONTENT_DATE.replace(tzinfo=UTC)
        assert record.metadata == {
            "imap_file_id": 7,
            "imap_hash": "imap-identity-hash",
            "descriptor": "imap_mag_l1_test",
            "version": 1,
            "version_major": 1,
            "content_date": "2026-03-04T00:00:00",
            "software_version": "1.0.0",
            "file_meta": {"source": "unit"},
        }
        assert Call("register_file", "hk/a_v001.csv") in hp.calls

    def test_identical_file_is_unchanged(self, sync, hp, store):
        _write(store, "hk/a.csv", b"data")
        snapshot = FileSnapshot.from_file(_file("hk/a.csv"))
        sync.sync_file(snapshot)
        hp.reset_calls()

        assert sync.sync_file(snapshot) == SyncAction.UNCHANGED
        assert hp.calls == [Call("get_file_by_path", "hk/a.csv")]

    def test_changed_columns_update_the_metadata(self, sync, hp, store):
        _write(store, "hk/a.csv", b"data")
        sync.sync_file(FileSnapshot.from_file(_file("hk/a.csv")))

        changed = FileSnapshot.from_file(_file("hk/a.csv", file_meta={"x": 1}))
        assert sync.sync_file(changed) == SyncAction.METADATA
        assert hp.record_at("hk/a.csv").metadata["file_meta"] == {"x": 1}

    def test_changed_content_overwrites(self, sync, hp, store):
        _write(store, "hk/a.csv", b"old")
        sync.sync_file(FileSnapshot.from_file(_file("hk/a.csv")))
        digest, _ = _write(store, "hk/a.csv", b"new content")

        snapshot = FileSnapshot.from_file(_file("hk/a.csv", hash="new-identity"))
        assert sync.sync_file(snapshot) == SyncAction.UPDATED
        assert hp.record_at("hk/a.csv").hash == digest
        assert hp.record_at("hk/a.csv").metadata["imap_hash"] == "new-identity"
        assert Call("add_file", "hk/a.csv") in hp.calls

    def test_changed_content_already_seen_by_heliopuddle_is_accepted(
        self, sync, hp, store
    ):
        old, size = _write(store, "hk/a.csv", b"old")
        hp.seed("hk/a.csv", hash=old, size=size, state="unknown")
        digest, _ = _write(store, "hk/a.csv", b"new content")

        assert (
            sync.sync_file(FileSnapshot.from_file(_file("hk/a.csv")))
            == SyncAction.UPDATED
        )
        assert hp.record_at("hk/a.csv").hash == digest
        assert Call("accept_file", "hk/a.csv") in hp.calls
        assert Call("add_file", "hk/a.csv") not in hp.calls

    def test_deleted_file_is_removed(self, sync, hp, store):
        _write(store, "hk/a.csv", b"data")
        sync.sync_file(FileSnapshot.from_file(_file("hk/a.csv")))

        deleted = _file("hk/a.csv", deletion_date=datetime(2026, 5, 1))
        assert sync.sync_file(FileSnapshot.from_file(deleted)) == SyncAction.REMOVED
        assert "hk/a.csv" not in hp.live_paths()

    def test_deleted_file_not_in_heliopuddle_is_unchanged(self, sync, hp):
        deleted = _file("hk/gone.csv", deletion_date=datetime(2026, 5, 1))
        assert sync.sync_file(FileSnapshot.from_file(deleted)) == SyncAction.UNCHANGED

    def test_path_outside_the_data_store_is_skipped(self, sync, hp):
        snapshot = FileSnapshot.from_file(_file("/archive/hk/a.csv"))
        assert sync.sync_file(snapshot) == SyncAction.SKIPPED
        assert hp.calls == []

    def test_missing_local_file_fails(self, sync):
        with pytest.raises(FileNotFoundError):
            sync.sync_file(FileSnapshot.from_file(_file("hk/missing.csv")))

    def test_store_content_not_matching_fails(self, sync, hp, store):
        _write(store, "hk/a.csv", b"data")
        hp.store = store / "elsewhere"  # heliopuddle sees no object: unknown

        with pytest.raises(RuntimeError, match="unknown"):
            sync.sync_file(FileSnapshot.from_file(_file("hk/a.csv")))

    def test_busy_path_is_retried(self, sync, hp, store):
        _write(store, "hk/a.csv", b"data")
        hp.inject_busy("hk/a.csv", times=2)

        assert (
            sync.sync_file(FileSnapshot.from_file(_file("hk/a.csv")))
            == SyncAction.ADDED
        )

    def test_busy_path_gives_up_after_the_configured_retries(self, store, hp):
        sync = HeliopuddleSync(
            _config(busy_retries=1), store, client=hp, sleep=lambda _: None
        )
        _write(store, "hk/a.csv", b"data")
        hp.inject_busy("hk/a.csv", times=5)

        from heliopuddle.client import BusyError

        with pytest.raises(BusyError):
            sync.sync_file(FileSnapshot.from_file(_file("hk/a.csv")))

    def test_unshared_store_uploads_the_content(self, store, hp):
        sync = HeliopuddleSync(
            _config(shared_datastore=False), store, client=hp, sleep=lambda _: None
        )
        _write(store, "hk/a.csv", b"data")

        assert (
            sync.sync_file(FileSnapshot.from_file(_file("hk/a.csv")))
            == SyncAction.ADDED
        )
        assert Call("add_file", "hk/a.csv") in hp.calls

    def test_dry_run_changes_nothing(self, sync, hp, store):
        _write(store, "hk/a.csv", b"data")
        snapshot = FileSnapshot.from_file(_file("hk/a.csv"))

        assert sync.sync_file(snapshot, dry_run=True) == SyncAction.ADDED
        assert hp.live_paths() == []


class TestApplyChanges:
    def test_failures_raise_after_trying_every_file(self, sync, hp, store):
        _write(store, "hk/a.csv", b"a")
        _write(store, "hk/b.csv", b"b")
        hp.inject_error(
            "register_file", RuntimeError("heliopuddle is down"), path="hk/a.csv"
        )

        with pytest.raises(HeliopuddleSyncError) as info:
            sync.apply_changes(
                [
                    FileSnapshot.from_file(_file("hk/a.csv")),
                    FileSnapshot.from_file(_file("hk/b.csv")),
                ]
            )

        assert list(info.value.failures) == ["hk/a.csv"]
        assert "hk/b.csv" in hp.live_paths()

    def test_failures_are_only_logged_without_fail_on_error(self, store, hp, caplog):
        sync = HeliopuddleSync(_config(fail_on_error=False), store, client=hp)
        hp.inject_error(
            "register_file", RuntimeError("heliopuddle is down"), path="hk/a.csv"
        )
        _write(store, "hk/a.csv", b"a")

        summary = sync.apply_changes([FileSnapshot.from_file(_file("hk/a.csv"))])
        assert list(summary.failures) == ["hk/a.csv"]
        assert "out of sync" in caplog.text


# -- the database hook ---------------------------------------------------------------


class TestDatabaseHook:
    def test_inserted_file_is_registered_after_commit(self, db, hp, store):
        _write(store, "hk/a.csv", b"data")
        db.upsert_file(_file("hk/a.csv"))

        record = hp.record_at("hk/a.csv")
        assert record.metadata["imap_file_id"] == db.get_files()[0].id

    def test_merged_update_and_soft_delete_follow(self, db, hp, store):
        _write(store, "hk/a.csv", b"data")
        db.upsert_file(_file("hk/a.csv"))

        db.upsert_file(_file("hk/a.csv", file_meta={"updated": True}))
        assert hp.record_at("hk/a.csv").metadata["file_meta"] == {"updated": True}

        file = db.get_files()[0]
        file.set_deleted()
        db.save(file)
        assert "hk/a.csv" not in hp.live_paths()
        # In a shared store heliopuddle deletes the object too, as IMAP itself would.
        assert not (store / "hk/a.csv").exists()

        _write(store, "hk/a.csv", b"data")  # the file comes back
        file.deletion_date = None
        db.save(file)
        assert "hk/a.csv" in hp.live_paths()

    def test_unchanged_upsert_makes_no_heliopuddle_calls(self, db, hp, store):
        _write(store, "hk/a.csv", b"data")
        db.upsert_file(_file("hk/a.csv"))
        hp.reset_calls()

        db.upsert_file(_file("hk/a.csv"))
        assert hp.calls == []

    def test_archive_adds_the_new_path_and_removes_the_old(self, db, hp, store):
        _write(store, "hk/a.csv", b"data")
        db.upsert_file(_file("hk/a.csv"))
        file = db.get_files()[0]
        _write(store, "archive/hk/a.csv", b"data")

        archived = file.archive_to_new_file_path(Path("archive/hk/a.csv"))
        db.upsert_files([archived, file])

        assert set(hp.live_paths()) == {"archive/hk/a.csv"}

    def test_other_tables_make_no_heliopuddle_calls(self, db, hp):
        db.save(db.get_workflow_progress("something"))
        assert hp.calls == []

    def test_rolled_back_changes_are_not_synced(self, db, hp, store):
        _write(store, "hk/a.csv", b"data")
        with pytest.raises(Exception):
            db.upsert_files([_file("hk/a.csv"), _file("hk/b.csv", name=None)])
        assert hp.calls == []

    def test_sync_failure_raises_but_keeps_the_database_change(self, db, hp, store):
        _write(store, "hk/a.csv", b"data")
        hp.inject_error(
            "register_file", RuntimeError("heliopuddle is down"), path="hk/a.csv"
        )

        with pytest.raises(HeliopuddleSyncError):
            db.upsert_file(_file("hk/a.csv"))

        assert [f.path for f in db.get_files()] == ["hk/a.csv"]

    def test_disabled_sync_makes_no_calls(self, tmp_path, hp, store):
        database = Database(
            db_url=f"sqlite:///{tmp_path}/off.db", heliopuddle_sync=False
        )
        Base.metadata.create_all(database.engine)
        _write(store, "hk/a.csv", b"data")
        database.upsert_file(_file("hk/a.csv"))
        assert hp.calls == []
        assert database.get_heliopuddle_sync() is None

    def test_default_sync_comes_from_app_settings(self, tmp_path):
        database = Database(db_url=f"sqlite:///{tmp_path}/default.db")
        assert database.get_heliopuddle_sync() is None  # disabled in the config


# -- migration and reconciliation -----------------------------------------------------


@pytest.fixture
def offline_db(tmp_path: Path) -> Database:
    """A database whose own writes are not synced, to set up IMAP-only state."""
    database = Database(db_url=f"sqlite:///{tmp_path}/imap.db", heliopuddle_sync=False)
    Base.metadata.create_all(database.engine)
    return database


class TestReconcile:
    def test_full_reconcile_migrates_every_file(self, sync, hp, store, offline_db):
        for name in ("a", "b", "c"):
            _write(store, f"hk/{name}.csv", name.encode())
            offline_db.upsert_file(_file(f"hk/{name}.csv", size=1))
        offline_db.upsert_file(_file("hk/old.csv", deletion_date=datetime(2026, 1, 1)))

        summary = sync.reconcile(offline_db)

        assert set(hp.live_paths()) == {"hk/a.csv", "hk/b.csv", "hk/c.csv"}
        assert summary.actions[SyncAction.ADDED] == 3
        assert summary.actions[SyncAction.UNCHANGED] == 1

    def test_full_reconcile_skips_files_already_in_sync_without_reading_them(
        self, sync, hp, store, offline_db
    ):
        _write(store, "hk/a.csv", b"a")
        offline_db.upsert_file(_file("hk/a.csv", size=1))
        sync.reconcile(offline_db)
        (store / "hk/a.csv").unlink()  # reading it now would fail
        hp.reset_calls()

        summary = sync.reconcile(offline_db)

        assert summary.actions == {SyncAction.UNCHANGED: 1}
        assert hp.methods_called() == ["find"]

    def test_full_reconcile_removes_files_deleted_in_imap(
        self, sync, hp, store, offline_db
    ):
        digest, size = _write(store, "hk/a.csv", b"a")
        hp.seed("hk/a.csv", hash=digest, size=size)
        offline_db.upsert_file(_file("hk/a.csv", deletion_date=datetime(2026, 1, 1)))

        summary = sync.reconcile(offline_db)
        assert hp.live_paths() == []
        assert summary.actions[SyncAction.REMOVED] == 1

    def test_untracked_files_are_reported_or_removed(self, sync, hp, offline_db):
        hp.seed("other/x.csv", hash="0" * 32, size=1)

        summary = sync.reconcile(offline_db)
        assert summary.untracked == ["other/x.csv"]
        assert "other/x.csv" in hp.live_paths()

        summary = sync.reconcile(offline_db, remove_untracked=True)
        assert hp.live_paths() == []
        assert summary.actions[SyncAction.REMOVED] == 1

    def test_incremental_reconcile_only_syncs_recent_changes(
        self, sync, hp, store, offline_db
    ):
        _write(store, "hk/old.csv", b"o")
        offline_db.upsert_file(
            _file("hk/old.csv", size=1, last_modified_date=datetime(2020, 1, 1))
        )
        _write(store, "hk/new.csv", b"n")
        offline_db.upsert_file(
            _file("hk/new.csv", size=1, last_modified_date=datetime(2026, 6, 1))
        )

        summary = sync.reconcile(offline_db, modified_after=datetime(2026, 1, 1))
        assert set(hp.live_paths()) == {"hk/new.csv"}
        assert summary.actions == {SyncAction.ADDED: 1}

    def test_reconcile_failures_raise_after_every_file(
        self, sync, hp, store, offline_db
    ):
        for name in ("a", "b"):
            _write(store, f"hk/{name}.csv", name.encode())
            offline_db.upsert_file(_file(f"hk/{name}.csv", size=1))
        hp.inject_error(
            "register_file", RuntimeError("heliopuddle is down"), path="hk/a.csv"
        )

        with pytest.raises(HeliopuddleSyncError) as info:
            sync.reconcile(offline_db)
        assert list(info.value.failures) == ["hk/a.csv"]
        assert "hk/b.csv" in hp.live_paths()


class TestRunSync:
    @pytest.fixture
    def settings(self, store, monkeypatch, sync):
        from imap_mag.config.AppSettings import AppSettings

        settings = AppSettings(data_store=store)  # type: ignore
        settings.heliopuddle = _config()
        monkeypatch.setattr(HeliopuddleSync, "from_settings", lambda _s: sync)
        return settings

    def test_first_run_is_full_and_records_progress(
        self, settings, hp, store, offline_db
    ):
        _write(store, "hk/a.csv", b"a")
        offline_db.upsert_file(
            _file("hk/a.csv", size=1, last_modified_date=datetime(2000, 1, 1))
        )
        now = datetime(2026, 7, 1, tzinfo=UTC)

        run_sync(settings, offline_db, now=now)

        assert "hk/a.csv" in hp.live_paths()
        progress = offline_db.get_workflow_progress(PROGRESS_ITEM_NAME)
        assert progress.progress_timestamp == datetime(2026, 7, 1)
        assert hp.closed

    def test_later_runs_are_incremental_from_the_last_run(
        self, settings, hp, store, offline_db
    ):
        run_sync(settings, offline_db, now=datetime(2026, 7, 1, tzinfo=UTC))
        hp.reset_calls()

        run_sync(settings, offline_db, now=datetime(2026, 7, 2, tzinfo=UTC))
        assert "find" not in hp.methods_called()

    def test_dry_run_records_no_progress(self, settings, offline_db):
        run_sync(settings, offline_db, dry_run=True)
        progress = offline_db.get_workflow_progress(PROGRESS_ITEM_NAME)
        assert progress.progress_timestamp is None

    def test_disabled_integration_is_an_error(self, store, offline_db):
        from imap_mag.config.AppSettings import AppSettings

        settings = AppSettings(data_store=store)  # type: ignore
        with pytest.raises(ValueError, match="disabled"):
            run_sync(settings, offline_db)


# -- datastore operations end to end ---------------------------------------------------


class TestDatastoreOperations:
    @pytest.fixture
    def manager(self, store, db):
        from imap_mag.config.AppSettings import AppSettings
        from imap_mag.io.DBIndexedDatastoreFileManager import (
            DBIndexedDatastoreFileManager,
        )

        settings = AppSettings(data_store=store)  # type: ignore
        return DBIndexedDatastoreFileManager(database=db, settings=settings)

    def test_delete_file_removes_it_from_heliopuddle(self, manager, db, hp, store):
        _write(store, "hk/a.csv", b"data")
        db.upsert_file(_file("hk/a.csv"))

        manager.delete_file(db.get_files()[0])

        assert not (store / "hk/a.csv").exists()
        assert hp.live_paths() == []

    def test_archive_inside_the_store_moves_the_file_in_heliopuddle(
        self, manager, db, hp, store
    ):
        _write(store, "hk/a.csv", b"data")
        db.upsert_file(_file("hk/a.csv"))

        manager.archive_file(db.get_files()[0], store / "archive")

        assert set(hp.live_paths()) == {"archive/hk/a.csv"}
        assert not (store / "hk/a.csv").exists()

    def test_archive_finishes_even_when_heliopuddle_fails(
        self, manager, db, hp, store, tmp_path
    ):
        _write(store, "hk/a.csv", b"data")
        db.upsert_file(_file("hk/a.csv"))
        hp.inject_error(
            "remove_file", RuntimeError("heliopuddle is down"), path="hk/a.csv"
        )

        with pytest.raises(HeliopuddleSyncError):
            manager.archive_file(db.get_files()[0], tmp_path / "archive")

        assert not (store / "hk/a.csv").exists()
        assert (tmp_path / "archive/hk/a.csv").exists()
        assert db.get_files(File.deletion_date.is_(None))[0].path.endswith(
            "archive/hk/a.csv"
        )

    def test_restoring_a_deleted_record_adds_it_back(self, manager, db, hp, store):
        from imap_mag.io.file import HKBinaryPathHandler

        _write(store, "hk/a.csv", b"data")
        db.upsert_file(_file("hk/a.csv"))
        file = db.get_files()[0]
        file.set_deleted()
        db.save(file)
        assert hp.live_paths() == []
        _write(store, "hk/a.csv", b"data")  # the file reappears in the data store

        handler = HKBinaryPathHandler(
            instrument="mag", descriptor="hsk-pw", content_date=CONTENT_DATE
        )
        manager.index_existing_file(store / "hk/a.csv", handler)

        assert "hk/a.csv" in hp.live_paths()


class TestCli:
    def test_sync_command_fails_when_disabled(self):
        from typer.testing import CliRunner

        from imap_mag.main import app

        result = CliRunner().invoke(app, ["heliopuddle", "sync"])
        assert result.exit_code == 1
