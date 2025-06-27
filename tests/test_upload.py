import json
import os
from pathlib import Path

import pytest
from typer.testing import CliRunner
from wiremock.client import (
    HttpMethods,
    Mapping,
    MappingRequest,
    MappingResponse,
)

from imap_mag.api.upload import upload
from imap_mag.main import app
from prefect_server.uploadFlow import upload_flow
from tests.util.miscellaneous import (
    DATASTORE,
    set_env,
    tidyDataFolders,  # noqa: F401
)
from tests.util.prefect import prefect_test_fixture  # noqa: F401

runner = CliRunner()


def add_mapping_for_successful_sdc_upload(wiremock_manager, upload_file: Path):
    """Add WireMock mapping for a successful SDC upload."""

    aws_url = f"https://s3.us-west-2.amazonaws.com/imap/mag/{upload_file.as_posix()}?some-amazon-s3-query-params=12345"

    wiremock_manager.add_string_mapping(
        f"/upload/{upload_file.name}",
        json.dumps(aws_url),
    )
    wiremock_manager.add_mapping(
        Mapping(
            request=MappingRequest(
                method=HttpMethods.PUT,
                url=aws_url,
            ),
            response=MappingResponse(
                status=200,
                body="{}",
            ),
            persistent=False,
        )
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
def test_upload_file_to_sdc(wiremock_manager, capture_cli_logs):
    # Set up.
    upload_file = Path("imap_mag_l1c_norm-mago_20251017_v001.cdf")
    add_mapping_for_successful_sdc_upload(wiremock_manager, upload_file)

    # Exercise.
    with (
        set_env("MAG_DATA_STORE", str(DATASTORE)),
        set_env("MAG_UPLOAD_API_URL_BASE", wiremock_manager.get_url()),
    ):
        upload([upload_file], auth_code="12345")

    # Verify.
    assert f"Uploading 1 files: {upload_file}" in capture_cli_logs.text
    assert (
        f"Found 1 files for upload: {DATASTORE / Path('imap/mag/l1c/2025/10') / upload_file}"
        in capture_cli_logs.text
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
def test_failed_sdc_file_upload(wiremock_manager, capture_cli_logs):
    # Set up.
    upload_file1 = Path("imap_mag_l1c_norm-mago_20251017_v001.cdf")
    add_mapping_for_successful_sdc_upload(wiremock_manager, upload_file1)

    upload_file2 = Path("imap_mag_l1b_norm-mago_20251017_v001.cdf")
    wiremock_manager.add_mapping(
        Mapping(
            request=MappingRequest(
                method=HttpMethods.GET,
                url=f"/upload/{upload_file2.name}",
            ),
            response=MappingResponse(
                status=409,  # failed upload
                body="{}",
            ),
            persistent=False,
        )
    )

    # Exercise and verify.
    with (
        pytest.raises(
            RuntimeError,
            match="Failed to upload 1 files.",
        ),
        set_env("MAG_DATA_STORE", str(DATASTORE)),
        set_env("MAG_UPLOAD_API_URL_BASE", wiremock_manager.get_url()),
    ):
        upload([upload_file1, upload_file2], auth_code="12345")

    assert (
        f"Failed to upload file {DATASTORE / Path('imap/mag/l1b/2025/10') / upload_file2}"
        in capture_cli_logs.text
    )
    assert (
        "Failed to upload 1 files. Only 1 files uploaded successfully."
        in capture_cli_logs.text
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
def test_upload_file_to_sdc_cli(wiremock_manager):
    # Set up.
    upload_file = Path("imap_mag_l1c_norm-mago_20251017_v001.cdf")
    add_mapping_for_successful_sdc_upload(wiremock_manager, upload_file)

    # Exercise.
    result = runner.invoke(
        app,
        ["upload", str(upload_file), "--auth-code", "12345"],
        env={
            "MAG_DATA_STORE": str(DATASTORE),
            "MAG_UPLOAD_API_URL_BASE": wiremock_manager.get_url(),
        },
    )

    # Verify.
    assert result.exit_code == 0

    assert f"Uploading 1 files: {upload_file}" in result.output
    assert (
        f"Found 1 files for upload: {DATASTORE / Path('imap/mag/l1c/2025/10') / upload_file}"
        in result.output
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_upload_flow_to_sdc(wiremock_manager, capture_cli_logs):
    # Set up.
    upload_file = Path("imap_mag_l1c_norm-mago_20251017_v001.cdf")
    add_mapping_for_successful_sdc_upload(wiremock_manager, upload_file)

    # Exercise.
    with (
        set_env("MAG_DATA_STORE", str(DATASTORE)),
        set_env("MAG_UPLOAD_API_URL_BASE", wiremock_manager.get_url()),
        set_env("SDC_AUTH_CODE", "12345"),
    ):
        await upload_flow([upload_file])

    # Verify.
    assert f"Uploading 1 files: {upload_file}" in capture_cli_logs.text
