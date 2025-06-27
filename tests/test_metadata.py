from datetime import datetime

from imap_mag.io.StandardSPDFMetadataProvider import StandardSPDFMetadataProvider


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


def test_metadata_recovers_correct_values_from_ancillary_file_without_level():
    filename = "imap_mag_l2-norm-offsets_20251017_20251017_v001.cdf"
    provider = StandardSPDFMetadataProvider.from_filename(filename)

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
    filename = "imap_mag_l2-rotation_20251017_v001.cdf"
    provider = StandardSPDFMetadataProvider.from_filename(filename)

    assert provider is not None
    assert provider.mission == "imap"
    assert provider.instrument == "mag"
    assert provider.level is None
    assert provider.descriptor == "l2-rotation"
    assert provider.content_date == datetime(2025, 10, 17)
    assert provider.end_date is None
    assert provider.version == 1
    assert provider.extension == "cdf"

    # Check the generated filename matches the original
    assert provider.get_filename() == filename
