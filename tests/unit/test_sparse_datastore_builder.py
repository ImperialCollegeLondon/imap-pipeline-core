"""Unit tests for the SparseDatastoreBuilder."""

from datetime import datetime

from imap_mag.config.CalibrationCommandConfig import SparseDatastoreConfig
from imap_mag.util import ScienceMode
from mag_toolkit.calibration.SparseDatastoreBuilder import SparseDatastoreBuilder

DATE = datetime(2026, 1, 30)


def _write(path, text="x"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _make_source_datastore(root):
    # L1 science for the day (norm -> l1c), both sensors.
    _write(root / "science/mag/l1c/2026/01/imap_mag_l1c_norm-mago_20260130_v001.cdf")
    _write(root / "science/mag/l1c/2026/01/imap_mag_l1c_norm-magi_20260130_v001.cdf")
    # A day outside the window that must NOT be copied.
    _write(root / "science/mag/l1c/2026/02/imap_mag_l1c_norm-mago_20260215_v001.cdf")
    # Spacecraft + pivot HK for the day.
    _write(root / "hk/sc/l1/x285/2026/01/imap_sc_l1_x285_20260130_v001.csv")
    _write(
        root
        / "hk/lo/l1/pivot-platform-angle/2026/01/imap_lo_l1_pivot-platform-angle_20260130_v001.csv"
    )
    # Shared small inputs.
    _write(root / "calibration/inputs/Matrices/CalibrationMatricesV8.mat")
    _write(root / "calibration/inputs/Profiles/hi_profile.csv")
    _write(root / "calibration/calculated_offsets/leinweber/offsets_v003.csv")
    _write(root / "spice/spin/imap_2026_020_2026_040_01.spin")
    _write(root / "spice/activities/imap_2026_020_2026_040_hist_01.sff")
    # Metakernel + a referenced kernel.
    _write(
        root / "spice/mk/metakernel.txt",
        "\\begindata\n"
        "PATH_VALUES     = ( 'test/datastore/spice' )\n"
        "PATH_SYMBOLS    = ( 'KERNELS' )\n"
        "KERNELS_TO_LOAD = ( '$KERNELS/lsk/naif0012.tls' )\n",
    )
    _write(root / "spice/lsk/naif0012.tls", "leapseconds")


def test_builds_sparse_copy(tmp_path):
    source = tmp_path / "datastore"
    _make_source_datastore(source)
    target = tmp_path / "work" / "sparse"

    builder = SparseDatastoreBuilder(source, SparseDatastoreConfig(), 0.99)
    result = builder.build(
        target, [DATE], ScienceMode.Normal, "metakernel.txt", matrix_version=8
    )

    assert result == target
    # Per-day science + HK copied (day d; window +/-1 has no other data present).
    assert (
        target / "science/mag/l1c/2026/01/imap_mag_l1c_norm-mago_20260130_v001.cdf"
    ).exists()
    assert (
        target / "science/mag/l1c/2026/01/imap_mag_l1c_norm-magi_20260130_v001.cdf"
    ).exists()
    assert (target / "hk/sc/l1/x285/2026/01/imap_sc_l1_x285_20260130_v001.csv").exists()
    assert (
        target
        / "hk/lo/l1/pivot-platform-angle/2026/01/imap_lo_l1_pivot-platform-angle_20260130_v001.csv"
    ).exists()
    # Out-of-window day NOT copied.
    assert not (
        target / "science/mag/l1c/2026/02/imap_mag_l1c_norm-mago_20260215_v001.cdf"
    ).exists()
    # Shared inputs copied.
    assert (target / "calibration/inputs/Matrices/CalibrationMatricesV8.mat").exists()
    assert (
        target / "calibration/calculated_offsets/leinweber/offsets_v003.csv"
    ).exists()
    assert (target / "spice/spin/imap_2026_020_2026_040_01.spin").exists()
    assert (target / "spice/activities/imap_2026_020_2026_040_hist_01.sff").exists()
    # Metakernel + referenced kernel copied.
    assert (target / "spice/mk/metakernel.txt").exists()
    assert (target / "spice/lsk/naif0012.tls").exists()


def test_lowercase_profiles_alias_added(tmp_path):
    source = tmp_path / "datastore"
    _make_source_datastore(source)
    target = tmp_path / "work" / "sparse"

    builder = SparseDatastoreBuilder(source, SparseDatastoreConfig(), 0.99)
    builder.build(
        target, [DATE], ScienceMode.Normal, "metakernel.txt", matrix_version=8
    )

    # MATLAB reads thruster profiles via lowercase inputs/profiles; the sparse copy
    # (on a case-sensitive FS) must expose that alias alongside Profiles.
    profiles_lower = target / "calibration/inputs/profiles"
    assert (target / "calibration/inputs/Profiles/hi_profile.csv").exists()
    assert (profiles_lower / "hi_profile.csv").exists()


def test_metakernel_path_values_rewritten_to_sparse_spice(tmp_path):
    source = tmp_path / "datastore"
    _make_source_datastore(source)
    target = tmp_path / "work" / "sparse"

    builder = SparseDatastoreBuilder(source, SparseDatastoreConfig(), 0.99)
    builder.build(
        target, [DATE], ScienceMode.Normal, "metakernel.txt", matrix_version=8
    )

    rewritten = (target / "spice/mk/metakernel.txt").read_text()
    # PATH_VALUES now points at the sparse spice folder (absolute) so kernels
    # resolve from the sparse root regardless of the original value.
    assert str((target / "spice").resolve()) in rewritten
    assert "test/datastore/spice" not in rewritten
    assert "$KERNELS/lsk/naif0012.tls" in rewritten  # entries unchanged


def test_parse_metakernel_kernels_strips_symbol_prefix(tmp_path):
    mk = tmp_path / "mk.txt"
    mk.write_text(
        "KERNELS_TO_LOAD = ( '$KERNELS/lsk/naif0012.tls',\n"
        "                    '$KERNELS/spk/de440.bsp' )\n"
    )
    kernels = SparseDatastoreBuilder._parse_metakernel_kernels(mk)
    assert kernels == ["lsk/naif0012.tls", "spk/de440.bsp"]
