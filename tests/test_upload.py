import json
import logging
import os
from pathlib import Path

import pytest
from wiremock.client import (
    HttpMethods,
    Mapping,
    MappingRequest,
    MappingResponse,
)

from imap_mag.api.upload import upload
from prefect_server.uploadFlow import upload_flow
from tests.util.miscellaneous import (
    DATASTORE,
    enableLogging,  # noqa: F401
    set_env,
    tidyDataFolders,  # noqa: F401
)
from tests.util.prefect import prefect_test_fixture  # noqa: F401


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
def test_upload_file(wiremock_manager, caplog):
    # Set up.
    upload_file = Path("imap_mag_l1c_norm-mago_20251017_v001.cdf")
    aws_url = "https://s3.us-west-2.amazonaws.com/imap/mag/l1c/2025/10/imap_mag_l1c_norm-mago_20251017_v001.cdf?some-amazon-s3-query-params=12345"

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

    caplog.set_level(logging.DEBUG)

    # Exercise.
    with (
        set_env("MAG_DATA_STORE", str(DATASTORE)),
        set_env("MAG_UPLOAD_API_URL_BASE", wiremock_manager.get_url()),
    ):
        upload([upload_file], auth_code="12345")

    # Verify.
    assert f"Uploading 1 files: {upload_file}" in caplog.text
    # assert (
    #     f"Found 1 files for upload: {DATASTORE / Path('imap/mag/l1c/2025/10') / upload_file}"
    #     in caplog.text
    # )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_upload_flow(wiremock_manager, caplog):
    # Set up.
    upload_file = Path("imap_mag_l1c_norm-mago_20251017_v001.cdf")
    aws_url = "https://s3.us-west-2.amazonaws.com/imap/mag/l1c/2025/10/imap_mag_l1c_norm-mago_20251017_v001.cdf?some-amazon-s3-query-params=12345"

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

    caplog.set_level(logging.DEBUG)

    # Exercise.
    with (
        set_env("MAG_DATA_STORE", str(DATASTORE)),
        set_env("MAG_UPLOAD_API_URL_BASE", wiremock_manager.get_url()),
        set_env("SDC_AUTH_CODE", "12345"),
    ):
        await upload_flow([upload_file])

    # Verify.
    assert f"Uploading 1 files: {upload_file}" in caplog.text
