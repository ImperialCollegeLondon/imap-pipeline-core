from datetime import datetime
from pathlib import Path

import pytest

from imap_mag.io.file import (
    SPICEPathHandler,
)


def test_path_handler_returns_correct_values_for_standard_spice_file():
    provider = SPICEPathHandler(
        kernel_folder="ck",
        filename="imap_dps_2025_281_2025_286_001.ah.bc",
    )

    assert provider.get_folder_structure() == "spice/ck"
    assert provider.get_filename() == "imap_dps_2025_281_2025_286_001.ah.bc"
    assert provider.supports_sequencing() is False

    assert provider.get_full_path() == Path(
        "spice/ck/imap_dps_2025_281_2025_286_001.ah.bc"
    )
    assert provider.get_full_path(Path("my_datastore")) == Path(
        "my_datastore/spice/ck/imap_dps_2025_281_2025_286_001.ah.bc"
    )


def test_spice_path_handler_fails_if_given_ancillary_file():
    filename = "imap_mag_l2-norm-offsets_20251017_20251017_v001.cdf"
    provider = SPICEPathHandler.from_filename(filename)
    assert provider is None, "SPICEPathHandler should not handle ancillary files."


@pytest.mark.parametrize(
    "filename",
    [
        "imap_sclk_0003.tsc",
        "imap_dps_2025_281_2025_286_001.ah.bcimap_2025_302_2025_303_001.ah.bc",
        "naif0012.tls",
        "imap_recon_20250925_20251104_v01.bsp",
        "imap_pred_od009_20251104_20251216_v01.bsp",
        "earth_000101_260128_251102.bpc",
        Path("ck/imap_2025_302_2025_303_001.ah.bc"),
        Path("sclk/imap_sclk_0032.tsc"),
    ],
)
def test_spice_path_handler_succeeds_if_given_spice_file(filename):
    provider = SPICEPathHandler.from_filename(filename)
    assert provider is not None
    assert (
        provider.get_filename() == filename
        if isinstance(filename, str)
        else filename.name
    )


def test_spice_path_handler_can_update_versions_of_metakernels():
    provider = SPICEPathHandler.from_filename(
        "imap_mag_metakernel_20251017000000_20251017235959_v001.tm"
    )
    assert provider is not None

    provider.increase_sequence()

    assert (
        provider.get_filename()
        == "imap_mag_metakernel_20251017000000_20251017235959_v002.tm"
    )


class TestGetKernelTypeFromFilename:
    def test_attitude_history_bc_file(self):
        kt = SPICEPathHandler.get_kernel_type_from_filename(
            "imap_2025_302_2025_303_001.ah.bc"
        )
        assert kt == "attitude_history"

    def test_spacecraft_clock_file(self):
        kt = SPICEPathHandler.get_kernel_type_from_filename("imap_sclk_0032.tsc")
        assert kt == "spacecraft_clock"

    def test_leapseconds_file(self):
        kt = SPICEPathHandler.get_kernel_type_from_filename("naif0012.tls")
        assert kt == "leapseconds"

    def test_ephemeris_reconstructed_file(self):
        kt = SPICEPathHandler.get_kernel_type_from_filename(
            "imap_recon_20250925_20251104_v01.bsp"
        )
        assert kt == "ephemeris_reconstructed"

    def test_pointing_attitude_takes_priority_over_attitude_history(self):
        kt = SPICEPathHandler.get_kernel_type_from_filename(
            "imap_dps_2025_281_2025_286_001.ah.bc"
        )
        assert kt == "pointing_attitude"

    def test_returns_none_for_unknown_file(self):
        kt = SPICEPathHandler.get_kernel_type_from_filename("some_unknown_file.txt")
        assert kt is None

    def test_works_with_path_object(self):
        kt = SPICEPathHandler.get_kernel_type_from_filename(
            Path("spice/lsk/naif0012.tls")
        )
        assert kt == "leapseconds"

    def test_science_frames_is_more_specific_than_imap_frames(self):
        kt = SPICEPathHandler.get_kernel_type_from_filename("imap_science_0001.tf")
        assert kt == "science_frames"

    def test_attitude_predict_file(self):
        kt = SPICEPathHandler.get_kernel_type_from_filename(
            "imap_2025_302_2025_303_001.ap.bc"
        )
        assert kt == "attitude_predict"


class TestGetUnsequencedPattern:
    def test_raises_for_non_versioned_spice_file(self):
        handler = SPICEPathHandler.from_filename("imap_dps_2025_281_2025_286_001.ah.bc")
        assert handler is not None
        assert not handler.is_versioned_spice_file

        with pytest.raises(ValueError, match="not versioned"):
            handler.get_unsequenced_pattern()

    def test_returns_pattern_for_v_prefixed_versioned_file(self):
        handler = SPICEPathHandler.from_filename(
            "imap_mag_metakernel_20251017000000_20251017235959_v001.tm"
        )
        assert handler is not None
        assert handler.is_versioned_spice_file

        pattern = handler.get_unsequenced_pattern()
        assert (
            pattern.match("imap_mag_metakernel_20251017000000_20251017235959_v003.tm")
            is not None
        )

    def test_raises_when_filename_does_not_end_with_version(self):
        handler = SPICEPathHandler(
            kernel_folder="mk",
            filename="some_file_without_version.tm",
        )
        handler.is_versioned_spice_file = True
        handler.version = 1

        with pytest.raises(ValueError):
            handler.get_unsequenced_pattern()


class TestAddMetadata:
    def test_sets_content_date_from_min_date_datetime(self):
        handler = SPICEPathHandler.from_filename("imap_dps_2025_281_2025_286_001.ah.bc")
        handler.add_metadata({"min_date_datetime": "2025-10-29 19:07:07"})
        assert handler.content_date == datetime(2025, 10, 29, 19, 7, 7)

    def test_falls_back_to_ingestion_date_when_no_min_date(self):
        handler = SPICEPathHandler.from_filename("imap_dps_2025_281_2025_286_001.ah.bc")
        handler.add_metadata({"ingestion_date": "2025-11-01, 08:05:12"})
        assert handler.content_date == datetime(2025, 11, 1, 8, 5, 12)

    def test_stores_full_metadata_dict(self):
        handler = SPICEPathHandler.from_filename("imap_dps_2025_281_2025_286_001.ah.bc")
        meta = {
            "min_date_datetime": "2025-10-29 19:07:07",
            "kernel_type": "attitude_history",
        }
        handler.add_metadata(meta)
        assert handler.get_metadata() == meta

    def test_updates_version_from_metadata(self):
        handler = SPICEPathHandler.from_filename("imap_dps_2025_281_2025_286_001.ah.bc")
        handler.add_metadata({"version": "42"})
        assert handler.version == 42

    def test_content_date_is_none_when_neither_date_field_present(self):
        handler = SPICEPathHandler.from_filename("imap_dps_2025_281_2025_286_001.ah.bc")
        handler.add_metadata({"some_other_field": "value"})
        assert handler.content_date is None

    def test_get_content_date_for_indexing_returns_content_date(self):
        handler = SPICEPathHandler.from_filename("imap_dps_2025_281_2025_286_001.ah.bc")
        handler.add_metadata({"min_date_datetime": "2025-10-29 19:07:07"})
        assert handler.get_content_date_for_indexing() == datetime(
            2025, 10, 29, 19, 7, 7
        )


class TestSequencing:
    def test_set_sequence_on_versioned_file_updates_version_and_filename(self):
        handler = SPICEPathHandler.from_filename(
            "imap_mag_metakernel_20251017000000_20251017235959_v001.tm"
        )
        assert handler is not None

        handler.set_sequence(5)

        assert handler.version == 5
        assert (
            handler.get_filename()
            == "imap_mag_metakernel_20251017000000_20251017235959_v005.tm"
        )

    def test_increase_sequence_on_versioned_file_increments_version(self):
        handler = SPICEPathHandler.from_filename(
            "imap_mag_metakernel_20251017000000_20251017235959_v003.tm"
        )
        assert handler is not None

        handler.increase_sequence()

        assert handler.version == 4
        assert (
            handler.get_filename()
            == "imap_mag_metakernel_20251017000000_20251017235959_v004.tm"
        )

    def test_set_sequence_raises_for_non_versioned_file(self):
        handler = SPICEPathHandler.from_filename("imap_dps_2025_281_2025_286_001.ah.bc")
        assert handler is not None
        assert not handler.is_versioned_spice_file

        with pytest.raises(ValueError, match="sequencing"):
            handler.set_sequence(2)

    def test_increase_sequence_raises_for_non_versioned_file(self):
        handler = SPICEPathHandler.from_filename("imap_dps_2025_281_2025_286_001.ah.bc")
        assert handler is not None

        with pytest.raises(ValueError, match="sequencing"):
            handler.increase_sequence()


class TestFromFilenameAdvanced:
    def test_returns_none_for_none_input(self):
        result = SPICEPathHandler.from_filename(None)
        assert result is None

    def test_path_in_spice_folder_structure_uses_folder_as_kernel_type(self):
        path = Path("spice/ck/imap_2025_302_2025_303_001.ah.bc")
        handler = SPICEPathHandler.from_filename(path)

        assert handler is not None
        assert handler.kernel_folder == "ck"
        assert handler.get_filename() == "imap_2025_302_2025_303_001.ah.bc"

    def test_versioned_file_detected_correctly(self):
        handler = SPICEPathHandler.from_filename(
            "imap_mag_metakernel_20251017000000_20251017235959_v001.tm"
        )
        assert handler is not None
        assert handler.is_versioned_spice_file is True
        assert handler.version == 1

    def test_non_versioned_file_is_not_marked_versioned(self):
        handler = SPICEPathHandler.from_filename("naif0012.tls")
        assert handler is not None
        assert handler.is_versioned_spice_file is False

    def test_path_with_wrong_folder_structure_falls_back_to_pattern_matching(self):
        path = Path("other/folder/imap_sclk_0032.tsc")
        handler = SPICEPathHandler.from_filename(path)

        assert handler is not None
        assert handler.kernel_folder == "sclk"


class TestParseMetakernelKernels:
    def test_strips_symbol_prefix(self, tmp_path):
        mk = tmp_path / "mk.txt"
        mk.write_text(
            "KERNELS_TO_LOAD = ( '$KERNELS/lsk/naif0012.tls',\n"
            "                    '$KERNELS/spk/de440.bsp' )\n"
        )
        kernels = SPICEPathHandler.parse_metakernel_kernels(mk)
        assert kernels == ["lsk/naif0012.tls", "spk/de440.bsp"]

    def test_no_kernels_to_load_block_returns_empty(self, tmp_path):
        mk = tmp_path / "mk.txt"
        mk.write_text("\\begintext\nNo kernels here.\n")
        assert SPICEPathHandler.parse_metakernel_kernels(mk) == []


class TestRewriteMetakernelPathValues:
    def test_normalises_path_values_to_relative_spice(self):
        text = (
            "PATH_VALUES     = ( '/some/absolute/datastore/spice' )\n"
            "KERNELS_TO_LOAD = ( '$KERNELS/lsk/naif0012.tls' )\n"
        )
        rewritten = SPICEPathHandler.rewrite_metakernel_path_values(text)
        assert "PATH_VALUES     = ( 'spice' )" in rewritten
        assert "/some/absolute/datastore/spice" not in rewritten
        assert "$KERNELS/lsk/naif0012.tls" in rewritten
