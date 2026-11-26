"""heliopuddle sync end to end: Postgres and a WireMock-stubbed heliopuddle API.

The heliopuddle client signs in as a service account (client credentials) at a
stubbed OIDC token endpoint, as it does against the platform's Keycloak.
"""

import base64
import hashlib
import json
from datetime import datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner
from wiremock.client import (
    HttpMethods,
    Mapping,
    MappingRequest,
    MappingResponse,
    Mappings,
    Requests,
)

from imap_db.model import File
from imap_mag.db import Database
from imap_mag.main import app
from imap_mag.util import Environment
from tests.util.database import test_database  # noqa: F401
from tests.util.WireMockManager import WireMockManager

PROJECT = "imap-mag"
TOKEN_PATH = "/realms/test/protocol/openid-connect/token"
TOKEN = "service-account-token"
PATH = "hk/mag/l1/hsk-pw/2026/03/imap_mag_l1_hsk-pw_20260304_v001.csv"
CONTENT = b"time,value\n1,2\n"
MD5 = hashlib.md5(CONTENT).hexdigest()


def _file() -> File:
    return File(
        name=PATH.rsplit("/", 1)[-1],
        path=PATH,
        descriptor="imap_mag_l1_hsk-pw",
        version=1,
        version_major=1,
        hash="identity-hash",
        size=len(CONTENT),
        content_date=datetime(2026, 3, 4),
        software_version="1.0.0",
    )


def _record(metadata: dict) -> dict:
    now = "2026-03-04T00:00:00Z"
    return {
        "id": 42,
        "project": PROJECT,
        "path": PATH,
        "name": PATH.rsplit("/", 1)[-1],
        "series_id": 1,
        "size": len(CONTENT),
        "hash": MD5,
        "hash_algo": "md5",
        "content_date": now,
        "metadata": metadata,
        "is_latest": True,
        "created_at": now,
        "updated_at": now,
        "storage_locations": [
            {"storage_location": "primary", "state": "available", "updated_at": now}
        ],
    }


def _stub(method: str, url_path: str, status: int, body: dict) -> None:
    Mappings.create_mapping(
        Mapping(
            request=MappingRequest(method=method, url_path=url_path),
            response=MappingResponse(
                status=status,
                json_body=body,
                headers={"Content-Type": "application/json"},
            ),
            persistent=False,
        )
    )


@pytest.fixture
def datastore(tmp_path: Path) -> Path:
    target = tmp_path / "datastore" / PATH
    target.parent.mkdir(parents=True)
    target.write_bytes(CONTENT)
    return tmp_path / "datastore"


@pytest.fixture
def heliopuddle_env(wiremock_manager: WireMockManager, datastore: Path, tmp_path):
    wiremock_manager.reset()
    Requests.reset_request_journal()
    base_url = wiremock_manager.get_url().rstrip("/")
    _stub(
        HttpMethods.GET,
        "/api/v1/auth/config",
        200,
        {"mode": "oidc", "token_url": base_url + TOKEN_PATH},
    )
    _stub(
        HttpMethods.POST,
        TOKEN_PATH,
        200,
        {"access_token": TOKEN, "token_type": "Bearer", "expires_in": 300},
    )
    with Environment(
        MAG_DATA_STORE=str(datastore),
        MAG_WORK_FOLDER=str(tmp_path / "work"),
        MAG_HELIOPUDDLE_ENABLED="true",
        MAG_HELIOPUDDLE_URL=base_url + "/api/v1",
        MAG_HELIOPUDDLE_PROJECT=PROJECT,
        MAG_HELIOPUDDLE_CLIENT_ID="imap-pipeline",
        MAG_HELIOPUDDLE_CLIENT_SECRET="a-secret",
    ):
        yield


def _assert_signed_in_as_the_service_account() -> None:
    [token_request] = _requests("POST", TOKEN_PATH)
    assert "grant_type=client_credentials" in token_request.body
    expected = "Basic " + base64.b64encode(b"imap-pipeline:a-secret").decode()
    assert token_request.headers["Authorization"] == expected
    api_calls = [
        r
        for r in Requests.get_all_received_requests().requests
        if r.request.url.startswith(f"/api/v1/projects/{PROJECT}")
    ]
    assert api_calls
    for call in api_calls:
        assert call.request.headers["Authorization"] == f"Bearer {TOKEN}"


def _requests(method: str, path_prefix: str) -> list:
    journal = Requests.get_all_received_requests()
    return [
        r.request
        for r in journal.requests
        if r.request.method == method and r.request.url.startswith(path_prefix)
    ]


def test_committed_file_is_registered_in_heliopuddle(
    heliopuddle_env,
    test_database,  # noqa: F811
):
    _stub(
        HttpMethods.GET,
        f"/api/v1/projects/{PROJECT}/files/by-path",
        404,
        {"detail": "no live file"},
    )
    expected_metadata = {"descriptor": "imap_mag_l1_hsk-pw"}

    # A fresh Database reads the integration settings from the environment.
    database = Database(db_url=test_database.engine.url)
    file = _file()
    _stub(
        HttpMethods.POST,
        f"/api/v1/projects/{PROJECT}/files",
        201,
        _record(expected_metadata),
    )
    # The register response lacks imap_file_id, so the metadata is then patched.
    _stub(HttpMethods.GET, "/api/v1/files/42", 200, _record(expected_metadata))
    _stub(
        HttpMethods.PATCH,
        "/api/v1/files/42/metadata",
        200,
        _record(expected_metadata | {"imap_file_id": 1}),
    )

    database.upsert_file(file)

    registers = _requests("POST", f"/api/v1/projects/{PROJECT}/files")
    assert len(registers) == 1
    body = json.loads(registers[0].body)
    assert body["path"] == PATH
    assert body["hash"] == MD5
    assert body["size"] == len(CONTENT)
    assert body["metadata"] == {
        "imap_file_id": database.get_files()[0].id,
        "descriptor": "imap_mag_l1_hsk-pw",
    }
    assert body["content_date"].startswith("2026-03-04T00:00:00")
    patches = _requests("PATCH", "/api/v1/files/42/metadata")
    assert (
        json.loads(patches[0].body)["imap_file_id"] == body["metadata"]["imap_file_id"]
    )
    _assert_signed_in_as_the_service_account()


def test_full_sync_command_migrates_existing_files(
    heliopuddle_env,
    test_database,  # noqa: F811
):
    offline = Database(db_url=test_database.engine.url, heliopuddle_sync=False)
    offline.upsert_file(_file())
    file_id = offline.get_files()[0].id
    metadata = {"imap_file_id": file_id, "descriptor": "imap_mag_l1_hsk-pw"}
    _stub(
        HttpMethods.GET,
        f"/api/v1/projects/{PROJECT}/files",
        200,
        {"items": [], "total": 0, "limit": 1000, "offset": 0},
    )
    _stub(
        HttpMethods.POST,
        f"/api/v1/projects/{PROJECT}/files",
        201,
        _record(metadata),
    )

    result = CliRunner().invoke(app, ["heliopuddle", "sync", "--full"])

    assert result.exit_code == 0, result.output
    registers = _requests("POST", f"/api/v1/projects/{PROJECT}/files")
    assert [json.loads(r.body)["path"] for r in registers] == [PATH]
    progress = offline.get_workflow_progress("heliopuddle-sync")
    assert progress.progress_timestamp is not None
    _assert_signed_in_as_the_service_account()
