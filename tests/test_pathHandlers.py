from datetime import datetime
from pathlib import Path

import pytest

from imap_mag.io import (
    FilePathHandlerSelector,
    NoProviderFoundError,
)
from imap_mag.io.file import (
    AncillaryPathHandler,
    CalibrationLayerPathHandler,
    HKBinaryPathHandler,
    HKDecodedPathHandler,
    SciencePathHandler,
)
from tests.util.miscellaneous import tidyDataFolders  # noqa: F401


def test_path_handler_returns_correct_values_for_standard_l2_file():
    provider = SciencePathHandler(
        mission="imap",
        instrument="mag",
        level="l2",
        descriptor="norm-mago",
        content_date=datetime(2025, 10, 17),
        version=1,
        extension="cdf",
    )

    assert provider.get_folder_structure() == "science/mag/l2/2025/10"
    assert provider.get_filename() == "imap_mag_l2_norm-mago_20251017_v001.cdf"
    assert provider.supports_sequencing() is True
    assert provider.get_unsequenced_pattern().pattern == (
        r"imap_mag_l2_norm\-mago_20251017_v(?P<version>\d+)\.cdf"
    )
    assert provider.get_full_path() == Path(
        "science/mag/l2/2025/10/imap_mag_l2_norm-mago_20251017_v001.cdf"
    )
    assert provider.get_full_path(Path("my_datastore")) == Path(
        "my_datastore/science/mag/l2/2025/10/imap_mag_l2_norm-mago_20251017_v001.cdf"
    )


def test_standard_path_handler_fails_if_given_ancillary_file():
    filename = "imap_mag_l2-norm-offsets_20251017_20251017_v001.cdf"
    provider = SciencePathHandler.from_filename(filename)
    assert provider is None, "SciencePathHandler should not handle ancillary files."


def test_ancillary_file_handler_gives_correct_unsequenced_pattern():
    provider = AncillaryPathHandler(
        mission="imap",
        instrument="mag",
        descriptor="l2-norm-offsets",
        start_date=datetime(2025, 10, 17),
        end_date=datetime(2025, 10, 17),
        version=1,
        extension="cdf",
    )

    assert provider.get_unsequenced_pattern().pattern == (
        r"imap_mag_l2\-norm\-offsets_20251017_20251017_v(?P<version>\d+)\.cdf"
    )


def test_ancillary_file_handler_gives_correct_unsequenced_pattern_without_end_date():
    provider = AncillaryPathHandler(
        mission="imap",
        instrument="mag",
        descriptor="l2-calibration",
        start_date=datetime(2025, 10, 17),
        end_date=None,
        version=1,
        extension="cdf",
    )

    assert provider.get_unsequenced_pattern().pattern == (
        r"imap_mag_l2\-calibration_20251017_v(?P<version>\d+)\.cdf"
    )


def test_get_filename_of_ancillary_path_handler_without_content_date_fails():
    provider = AncillaryPathHandler(
        mission="imap",
        instrument="mag",
        descriptor="l2-norm-offsets",
        end_date=datetime(2025, 10, 17),
        extension="cdf",
    )

    with pytest.raises(
        ValueError,
        match="No 'start_date' defined. Cannot generate file name.",
    ):
        provider.get_filename()


def test_get_unsequenced_pattern_of_ancillary_path_handler_without_content_date__fails():
    provider = AncillaryPathHandler(
        mission="imap",
        instrument="mag",
        descriptor="l2-norm-offsets",
        end_date=datetime(2025, 10, 17),
        extension="cdf",
    )

    with pytest.raises(
        ValueError,
        match="No 'start_date' defined. Cannot generate pattern.",
    ):
        provider.get_unsequenced_pattern()


def test_ancillary_from_filename_returns_none_if_filename_does_not_match_pattern():
    filename = "imap_mag_l2-notcalibration_20251017_v001.cdf"
    ancillary_provider = AncillaryPathHandler.from_filename(filename)
    assert ancillary_provider is None, (
        "Should return None for invalid ancillary filenames."
    )


def test_calibration_layer_get_equivalent_data_handler():
    # Set up.
    path_handler = CalibrationLayerPathHandler(
        version=2,
        descriptor="offsets",
        content_date=datetime(2025, 10, 4),
    )

    # Exercise.
    data_handler = path_handler.get_equivalent_data_handler()

    # Verify.
    assert data_handler == CalibrationLayerPathHandler(
        version=2,
        descriptor="offsets",
        extra_descriptor="-data",
        content_date=datetime(2025, 10, 4),
        extension="csv",
    )


@pytest.mark.parametrize(
    "path, expected_provider, provider_type",
    [
        (
            Path("imap/mag/l1b/2025/10/imap_mag_l1b_norm-mago_20251004_v002.cdf"),
            SciencePathHandler(
                version=2,
                level="l1b",
                descriptor="norm-mago",
                content_date=datetime(2025, 10, 4),
                extension="cdf",
            ),
            "SciencePathHandler",
        ),
        (
            Path(
                "imap/mag/calibration/layer/2025/10/imap_mag_offsets-layer_20251004_v002.json"
            ),
            CalibrationLayerPathHandler(
                version=2,
                descriptor="offsets",
                content_date=datetime(2025, 10, 4),
            ),
            "CalibrationLayerPathHandler",
        ),
    ],
)
def test_find_provider_by_path(
    capture_cli_logs, path, expected_provider, provider_type
):
    # Exercise.
    actual_provider = FilePathHandlerSelector.find_by_path(path)

    # Verify.
    assert actual_provider == expected_provider

    assert f"Path handler {provider_type} matches file {path}." in capture_cli_logs.text


@pytest.mark.parametrize(
    "provider, expected_folder_structure",
    (
        (
            HKBinaryPathHandler(
                descriptor="hsk-pw",
                content_date=datetime(2024, 12, 10),
            ),
            "hk/mag/l0/hsk-pw/2024/12",
        ),
        (
            HKDecodedPathHandler(
                descriptor="hsk-pw",
                content_date=datetime(2024, 12, 10),
            ),
            "hk/mag/l1/hsk-pw/2024/12",
        ),
        (
            SciencePathHandler(
                level="l1b",
                descriptor="mago-normal",
                content_date=datetime(2024, 12, 10),
            ),
            "science/mag/l1b/2024/12",
        ),
        (
            SciencePathHandler(
                level="l2",
                content_date=datetime(2024, 12, 10),
            ),
            "science/mag/l2/2024/12",
        ),
    ),
)
def test_get_folder_structure(provider, expected_folder_structure):
    # Exercise.
    actual_folder_structure = provider.get_folder_structure()

    # Verify.
    assert actual_folder_structure == expected_folder_structure


def test_get_folder_structure_error_on_no_date_and_level():
    with pytest.raises(
        ValueError,
        match="No 'content_date', 'level' defined. Cannot generate folder structure.",
    ):
        SciencePathHandler().get_folder_structure()


@pytest.mark.parametrize(
    "provider",
    (
        HKBinaryPathHandler(
            content_date=datetime(2024, 12, 10),
            part=3,
            extension="pkts",
        ),
        HKBinaryPathHandler(
            descriptor="hsk-pw",
            part=3,
            extension="pkts",
        ),
        HKBinaryPathHandler(
            descriptor="hsk-pw",
            content_date=datetime(2024, 12, 10),
            part=3,
        ),
        HKDecodedPathHandler(
            content_date=datetime(2024, 12, 10),
            version=3,
            extension="pkts",
        ),
        HKDecodedPathHandler(
            descriptor="hsk-pw",
            version=3,
            extension="pkts",
        ),
        HKDecodedPathHandler(
            descriptor="hsk-pw",
            content_date=datetime(2024, 12, 10),
            version=3,
        ),
    ),
)
def test_get_filename_error_on_no_required_parameter(provider):
    with pytest.raises(
        ValueError,
        match=r"No '[\w,']+' defined. Cannot generate file name.",
    ):
        provider.get_filename()


@pytest.mark.parametrize(
    "filename, expected",
    [
        (
            "imap_mag_l2-norm-offsets_20251017_20251017_v001.cdf",
            AncillaryPathHandler(
                descriptor="l2-norm-offsets",
                start_date=datetime(2025, 10, 17),
                end_date=datetime(2025, 10, 17),
                version=1,
                extension="cdf",
            ),
        ),
        (
            "imap_mag_l2-calibration_20251017_v001.cdf",
            AncillaryPathHandler(
                descriptor="l2-calibration",
                start_date=datetime(2025, 10, 17),
                end_date=None,
                version=1,
                extension="cdf",
            ),
        ),
        (
            "imap_mag_l0_hsk-pw_20241210_003.pkts",
            HKBinaryPathHandler(
                descriptor="hsk-pw",
                content_date=datetime(2024, 12, 10),
                part=3,
                extension="pkts",
            ),
        ),
        (
            "imap_mag_l1_hsk-pw_20251111_v002.pkts",
            HKDecodedPathHandler(
                descriptor="hsk-pw",
                content_date=datetime(2025, 11, 11),
                version=2,
                extension="pkts",
            ),
        ),
        (
            "imap_mag_l1b_norm-mago_20250502_v001.cdf",
            SciencePathHandler(
                level="l1b",
                descriptor="norm-mago",
                content_date=datetime(2025, 5, 2),
                version=1,
                extension="cdf",
            ),
        ),
        (
            "imap_mag_l2-pre_norm-mago_20251017_v001.cdf",
            SciencePathHandler(
                level="l2-pre",
                descriptor="norm-mago",
                content_date=datetime(2025, 10, 17),
                version=1,
                extension="cdf",
            ),
        ),
        (
            "imap_mag_l2_norm-mago_20251017_v001.cdf",
            SciencePathHandler(
                level="l2",
                descriptor="norm-mago",
                content_date=datetime(2025, 10, 17),
                version=1,
                extension="cdf",
            ),
        ),
        (
            "imap_mag_l2_norm-dsrf_20261231_v010.cdf",
            SciencePathHandler(
                level="l2",
                descriptor="norm-dsrf",
                content_date=datetime(2026, 12, 31),
                version=10,
                extension="cdf",
            ),
        ),
        (
            "imap_mag_l2_burst_20261231_v010.cdf",
            SciencePathHandler(
                level="l2",
                descriptor="burst",
                content_date=datetime(2026, 12, 31),
                version=10,
                extension="cdf",
            ),
        ),
        (
            "imap_mag_definitely_not_a_standard_spdf_file.txt",
            None,
        ),
    ],
)
def test_find_correct_provider_from_filename(filename, expected):
    actual = FilePathHandlerSelector.find_by_path(filename, throw_if_not_found=False)
    assert actual == expected


@pytest.mark.parametrize(
    "throw_error",
    [True, False],
)
def test_behavior_on_no_suitable_provider_found(capture_cli_logs, throw_error):
    # Setup.
    path = Path("this-is_not_a_supported_file_v002.cdf")

    # Exercise and verify.
    if throw_error:
        with pytest.raises(
            NoProviderFoundError,
            match=f"No suitable path handler found for file {path}.",
        ):
            FilePathHandlerSelector.find_by_path(path, throw_if_not_found=True)
    else:
        path_handler = FilePathHandlerSelector.find_by_path(
            path, throw_if_not_found=False
        )
        assert path_handler is None

    assert f"No suitable path handler found for file {path}." in capture_cli_logs.text
