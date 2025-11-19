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
