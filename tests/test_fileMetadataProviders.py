from datetime import datetime
from pathlib import Path

import pytest

from imap_mag.io import (
    CalibrationLayerMetadataProvider,
    FileMetadataProviders,
    NoProviderFoundError,
    StandardSPDFMetadataProvider,
)
from tests.util.miscellaneous import tidyDataFolders  # noqa: F401


@pytest.mark.parametrize(
    "path, expected_provider, provider_type",
    [
        (
            Path("imap/mag/l1b/2025/10/imap_mag_l1b_norm-mago_20251004_v002.cdf"),
            StandardSPDFMetadataProvider(
                version=2,
                level="l1b",
                descriptor="norm-mago",
                content_date=datetime(2025, 10, 4),
                extension="cdf",
            ),
            "StandardSPDFMetadataProvider",
        ),
        (
            Path(
                "imap/mag/calibration/layer/2025/10/imap_mag_offsets-layer_20251004_v002.json"
            ),
            CalibrationLayerMetadataProvider(
                version=2,
                calibration_descriptor="offsets",
                content_date=datetime(2025, 10, 4),
                extension="json",
            ),
            "CalibrationLayerMetadataProvider",
        ),
    ],
)
def test_find_provider_by_path(
    capture_cli_logs, path, expected_provider, provider_type
):
    # Exercise.
    actual_provider = FileMetadataProviders.find_by_path(path)

    # Verify.
    assert actual_provider == expected_provider

    assert (
        f"Metadata provider {provider_type} matches file {path}."
        in capture_cli_logs.text
    )


@pytest.mark.parametrize(
    "throw_error",
    [True, False],
)
def test_no_suitable_provider(capture_cli_logs, throw_error):
    # Setup.
    path = Path("imap_mag_this-is_not_a_supported_file_v002.cdf")

    # Exercise and verify.
    if throw_error:
        with pytest.raises(
            NoProviderFoundError,
            match=f"No suitable metadata provider found for file {path}.",
        ):
            FileMetadataProviders.find_by_path(path, throw_on_none_found=True)
    else:
        metadata_provider = FileMetadataProviders.find_by_path(
            path, throw_on_none_found=False
        )
        assert metadata_provider is None

    assert (
        f"No suitable metadata provider found for file {path}." in capture_cli_logs.text
    )
