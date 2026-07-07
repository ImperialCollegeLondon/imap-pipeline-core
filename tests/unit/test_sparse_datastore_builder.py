"""Unit tests for the SparseDatastoreBuilder."""

from datetime import datetime

from imap_mag.config.CalibrationCommandConfig import (
    SparseDatastoreConfig,
    SparseDatastorePattern,
)
from imap_mag.util import ScienceMode
from mag_toolkit.calibration.SparseDatastoreBuilder import SparseDatastoreBuilder

DATE = datetime(2026, 1, 30)


def _config() -> SparseDatastoreConfig:
    """A representative pattern set mirroring the yaml defaults."""
    return SparseDatastoreConfig(
        patterns=[
            SparseDatastorePattern(
                pattern="science/mag/{level}/%Y/%m/imap_mag_{level}_{mode}-mago_%Y%m%d_v*.cdf"
            ),
            SparseDatastorePattern(
                pattern="science/mag/{level}/%Y/%m/imap_mag_{level}_{mode}-magi_%Y%m%d_v*.cdf"
            ),
            SparseDatastorePattern(
                pattern="hk/lo/l1/pivot-platform-angle/%Y/%m/imap_lo_l1_pivot-platform-angle_%Y%m%d_v*.csv",
                days_before=1,
                days_after=1,
            ),
            SparseDatastorePattern(
                pattern="calibration/inputs/Matrices/CalibrationMatricesV{matrix_version}.mat"
            ),
            SparseDatastorePattern(pattern="calibration/inputs/Profiles/**/*"),
            SparseDatastorePattern(pattern="calibration/calculated_offsets/**/*"),
            SparseDatastorePattern(pattern="spice/spin/*"),
        ]
    )


def _write(path, text="x"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _make_source_datastore(root):
    # L1 science for the day (norm -> l1c), both sensors.
    _write(root / "science/mag/l1c/2026/01/imap_mag_l1c_norm-mago_20260130_v001.cdf")
    _write(root / "science/mag/l1c/2026/01/imap_mag_l1c_norm-magi_20260130_v001.cdf")
    # Science for a neighbouring day - must NOT be copied (science window is 0).
    _write(root / "science/mag/l1c/2026/01/imap_mag_l1c_norm-mago_20260129_v001.cdf")
    # Pivot HK for the day and the day before (pivot window is +/-1).
    _write(
        root
        / "hk/lo/l1/pivot-platform-angle/2026/01/imap_lo_l1_pivot-platform-angle_20260130_v001.csv"
    )
    _write(
        root
        / "hk/lo/l1/pivot-platform-angle/2026/01/imap_lo_l1_pivot-platform-angle_20260129_v001.csv"
    )
    # Shared small inputs.
    _write(root / "calibration/inputs/Matrices/CalibrationMatricesV8.mat")
    _write(root / "calibration/inputs/Matrices/CalibrationMatricesV9.mat")  # other ver
    _write(root / "calibration/inputs/Profiles/hi_profile.csv")
    _write(root / "calibration/calculated_offsets/leinweber/offsets_v003.csv")
    _write(root / "spice/spin/imap_2026_020_2026_040_01.spin")
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

    builder = SparseDatastoreBuilder(source, _config(), 0.99)
    result = builder.build(
        target, [DATE], ScienceMode.Normal, "metakernel.txt", matrix_version=8
    )

    assert result == target
    # Science + pivot for the day copied.
    assert (
        target / "science/mag/l1c/2026/01/imap_mag_l1c_norm-mago_20260130_v001.cdf"
    ).exists()
    assert (
        target / "science/mag/l1c/2026/01/imap_mag_l1c_norm-magi_20260130_v001.cdf"
    ).exists()
    # Only the calibration matrix version in use is copied.
    assert (target / "calibration/inputs/Matrices/CalibrationMatricesV8.mat").exists()
    assert not (
        target / "calibration/inputs/Matrices/CalibrationMatricesV9.mat"
    ).exists()
    # Shared inputs copied.
    assert (target / "calibration/inputs/Profiles/hi_profile.csv").exists()
    assert (
        target / "calibration/calculated_offsets/leinweber/offsets_v003.csv"
    ).exists()
    assert (target / "spice/spin/imap_2026_020_2026_040_01.spin").exists()
    # Metakernel + referenced kernel copied.
    assert (target / "spice/mk/metakernel.txt").exists()
    assert (target / "spice/lsk/naif0012.tls").exists()


def test_per_pattern_day_windows(tmp_path):
    source = tmp_path / "datastore"
    _make_source_datastore(source)
    target = tmp_path / "work" / "sparse"

    builder = SparseDatastoreBuilder(source, _config(), 0.99)
    builder.build(
        target, [DATE], ScienceMode.Normal, "metakernel.txt", matrix_version=8
    )

    # Science has a 0-day window: the neighbouring day is NOT copied.
    assert not (
        target / "science/mag/l1c/2026/01/imap_mag_l1c_norm-mago_20260129_v001.cdf"
    ).exists()
    # Pivot HK has a +/-1 window: the day before IS copied.
    assert (
        target
        / "hk/lo/l1/pivot-platform-angle/2026/01/imap_lo_l1_pivot-platform-angle_20260129_v001.csv"
    ).exists()
    assert (
        target
        / "hk/lo/l1/pivot-platform-angle/2026/01/imap_lo_l1_pivot-platform-angle_20260130_v001.csv"
    ).exists()


def test_metakernel_path_values_rewritten_to_relative_spice(tmp_path):
    source = tmp_path / "datastore"
    _make_source_datastore(source)
    target = tmp_path / "work" / "sparse"

    builder = SparseDatastoreBuilder(source, _config(), 0.99)
    builder.build(
        target, [DATE], ScienceMode.Normal, "metakernel.txt", matrix_version=8
    )

    rewritten = (target / "spice/mk/metakernel.txt").read_text()
    # PATH_VALUES normalised to the relative spice folder (short, avoids SPICE's
    # path-length limit) so it furnishes from the sparse root (cwd).
    assert "PATH_VALUES     = ( 'spice' )" in rewritten
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
