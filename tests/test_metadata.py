from datetime import datetime
from pathlib import Path

import pytest

from imap_mag.io import (
    AncillaryFileMetadataProvider,
    CalibrationLayerMetadataProvider,
    FileMetadataProviders,
    NoProviderFoundError,
    StandardSPDFMetadataProvider,
)
from tests.util.miscellaneous import tidyDataFolders  # noqa: F401


def test_metadata_provider_returns_correct_values_for_standard_l2_file():
    provider = StandardSPDFMetadataProvider(
        mission="imap",
        instrument="mag",
        level="l2",
        descriptor="norm-mago",
        content_date=datetime(2025, 10, 17),
        version=1,
        extension="cdf",
    )

    assert provider.get_folder_structure() == "imap/mag/l2/2025/10"
    assert provider.get_filename() == "imap_mag_l2_norm-mago_20251017_v001.cdf"
    assert provider.supports_versioning() is True
    assert provider.get_unversioned_pattern().pattern == (
        r"imap_mag_l2_norm-mago_20251017_v(?P<version>\d+)\.cdf"
    )


def test_metadata_recovers_correct_values_from_standard_l2_file():
    filename = "imap_mag_l2_norm-mago_20251017_v001.cdf"
    provider = StandardSPDFMetadataProvider.from_filename(filename)

    assert provider is not None
    assert provider.mission == "imap"
    assert provider.instrument == "mag"
    assert provider.level == "l2"
    assert provider.descriptor == "norm-mago"
    assert provider.content_date == datetime(2025, 10, 17)
    assert provider.version == 1
    assert provider.extension == "cdf"

    # Check the generated filename matches the original
    assert provider.get_filename() == filename


def test_metadata_recovers_correct_values_from_standard_l2_pre_file():
    filename = "imap_mag_l2-pre_norm-mago_20251017_v001.cdf"
    provider = StandardSPDFMetadataProvider.from_filename(filename)

    assert provider is not None
    assert provider.mission == "imap"
    assert provider.instrument == "mag"
    assert provider.level == "l2-pre"
    assert provider.descriptor == "norm-mago"
    assert provider.content_date == datetime(2025, 10, 17)
    assert provider.version == 1
    assert provider.extension == "cdf"

    # Check the generated filename matches the original
    assert provider.get_filename() == filename


def test_standard_metadata_provider_fails_if_given_ancillary_file():
    filename = "imap_mag_l2-norm-offsets_20251017_20251017_v001.cdf"
    provider = StandardSPDFMetadataProvider.from_filename(filename)
    assert provider is None, (
        "StandardSPDFMetadataProvider should not handle ancillary files."
    )


def test_metadata_recovers_correct_values_from_ancillary_file_without_level():
    filename = "imap_mag_l2-norm-offsets_20251017_20251017_v001.cdf"
    provider = AncillaryFileMetadataProvider.from_filename(filename)

    assert provider is not None
    assert provider.mission == "imap"
    assert provider.instrument == "mag"
    assert provider.level is None
    assert provider.descriptor == "l2-norm-offsets"
    assert provider.content_date == datetime(2025, 10, 17)
    assert provider.version == 1
    assert provider.extension == "cdf"
    assert provider.end_date == datetime(2025, 10, 17)

    # Check the generated filename matches the original
    assert provider.get_filename() == filename


def test_metadata_recovers_correct_values_from_ancillary_file_name_without_level_or_end_date():
    filename = "imap_mag_l2-calibration_20251017_v001.cdf"
    provider = AncillaryFileMetadataProvider.from_filename(filename)

    assert provider is not None
    assert provider.mission == "imap"
    assert provider.instrument == "mag"
    assert provider.level is None
    assert provider.descriptor == "l2-calibration"
    assert provider.content_date == datetime(2025, 10, 17)
    assert provider.end_date is None
    assert provider.version == 1
    assert provider.extension == "cdf"

    # Check the generated filename matches the original
    assert provider.get_filename() == filename


def test_ancillary_file_metadata_gives_correct_unversioned_pattern():
    provider = AncillaryFileMetadataProvider(
        mission="imap",
        instrument="mag",
        level=None,
        descriptor="l2-norm-offsets",
        content_date=datetime(2025, 10, 17),
        end_date=datetime(2025, 10, 17),
        version=1,
        extension="cdf",
    )

    assert provider.get_unversioned_pattern().pattern == (
        r"imap_mag_l2-norm-offsets_20251017_20251017_v(?P<version>\d+)\.cdf"
    )


def test_ancillary_file_metadata_gives_correct_unversioned_pattern_without_end_date():
    provider = AncillaryFileMetadataProvider(
        mission="imap",
        instrument="mag",
        level=None,
        descriptor="l2-calibration",
        content_date=datetime(2025, 10, 17),
        end_date=None,
        version=1,
        extension="cdf",
    )

    assert provider.get_unversioned_pattern().pattern == (
        r"imap_mag_l2-calibration_20251017_v(?P<version>\d+)\.cdf"
    )


def test_get_filename_of_ancillary_metadata_provider_without_content_date__fails():
    provider = AncillaryFileMetadataProvider(
        mission="imap",
        instrument="mag",
        level=None,
        descriptor="l2-norm-offsets",
        end_date=datetime(2025, 10, 17),
        extension="cdf",
    )

    with pytest.raises(ValueError) as exc_info:
        provider.get_filename()

    assert (
        str(exc_info.value)
        == "No 'descriptor', 'content_date', 'version', or 'extension' defined. Cannot generate file name."
    )


def test_get_unversioned_pattern_of_ancillary_metadata_provider_without_content_date__fails():
    provider = AncillaryFileMetadataProvider(
        mission="imap",
        instrument="mag",
        level=None,
        descriptor="l2-norm-offsets",
        end_date=datetime(2025, 10, 17),
        extension="cdf",
    )

    with pytest.raises(ValueError) as exc_info:
        provider.get_unversioned_pattern()

    assert (
        str(exc_info.value)
        == "No 'content_date' or 'descriptor' or 'extension' defined. Cannot generate pattern."
    )


def test_ancillary_from_filename_returns_none_if_filename_does_not_match_pattern():
    filename = "imap_mag_l2-notcalibration_20251017_v001.cdf"
    ancillary_provider = AncillaryFileMetadataProvider.from_filename(filename)
    assert ancillary_provider is None, (
        "Should return None for invalid ancillary filenames."
    )


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
