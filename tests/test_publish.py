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

from imap_mag.api.publish import publish
from imap_mag.main import app
from prefect_server.publishFlow import publish_flow
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
def test_publish_file_to_sdc(wiremock_manager, capture_cli_logs):
    # Set up.
    upload_file = Path("imap_mag_l1c_norm-mago_20251017_v001.cdf")
    add_mapping_for_successful_sdc_upload(wiremock_manager, upload_file)

    # Exercise.
    with (
        set_env("MAG_DATA_STORE", str(DATASTORE)),
        set_env("MAG_PUBLISH_API_URL_BASE", wiremock_manager.get_url()),
    ):
        publish([upload_file], auth_code="12345")

    # Verify.
    assert f"Publishing 1 files: {upload_file}" in capture_cli_logs.text
    assert (
        f"Found 1 files for publish: {DATASTORE / Path('science/mag/l1c/2025/10') / upload_file}"
        in capture_cli_logs.text
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
def test_failed_sdc_file_publish(wiremock_manager, capture_cli_logs):
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
                status=409,  # failed publish
                body="{}",
            ),
            persistent=False,
        )
    )

    # Exercise and verify.
    with (
        pytest.raises(
            RuntimeError,
            match="Failed to publish 1 files.",
        ),
        set_env("MAG_DATA_STORE", str(DATASTORE)),
        set_env("MAG_PUBLISH_API_URL_BASE", wiremock_manager.get_url()),
    ):
        publish([upload_file1, upload_file2], auth_code="12345")

    assert (
        f"Failed to publish file {DATASTORE / Path('science/mag/l1b/2025/10') / upload_file2}"
        in capture_cli_logs.text
    )
    assert (
        "Failed to publish 1 files. Only 1 files published successfully."
        in capture_cli_logs.text
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
def test_publish_file_to_sdc_cli(wiremock_manager):
    # Set up.
    upload_file = Path("imap_mag_l1c_norm-mago_20251017_v001.cdf")
    add_mapping_for_successful_sdc_upload(wiremock_manager, upload_file)

    # Exercise.
    result = runner.invoke(
        app,
        ["publish", str(upload_file), "--auth-code", "12345"],
        env={
            "MAG_DATA_STORE": str(DATASTORE),
            "MAG_PUBLISH_API_URL_BASE": wiremock_manager.get_url(),
        },
    )

    # Verify.
    assert result.exit_code == 0

    assert f"Publishing 1 files: {upload_file}" in result.output
    assert (
        f"Found 1 files for publish: {DATASTORE / Path('science/mag/l1c/2025/10') / upload_file}"
        in result.output
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_publish_flow_to_sdc(wiremock_manager, capture_cli_logs):
    # Set up.
    upload_file = Path("imap_mag_l1c_norm-mago_20251017_v001.cdf")
    add_mapping_for_successful_sdc_upload(wiremock_manager, upload_file)

    # Exercise.
    with (
        set_env("MAG_DATA_STORE", str(DATASTORE)),
        set_env("MAG_PUBLISH_API_URL_BASE", wiremock_manager.get_url()),
        set_env("SDC_AUTH_CODE", "12345"),
    ):
        await publish_flow([upload_file])

    # Verify.
    assert f"Publishing 1 files: {upload_file}" in capture_cli_logs.text
