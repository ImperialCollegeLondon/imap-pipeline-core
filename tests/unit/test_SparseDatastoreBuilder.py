"""Unit tests for SparseDatastoreBuilder."""

from pathlib import Path

from imap_mag.config.CalibrationCommandConfig import SparseDatastoreConfig
from mag_toolkit.calibration.SparseDatastoreBuilder import SparseDatastoreBuilder


def _make_source_datastore(base: Path) -> Path:
    """Create a source datastore with a metakernel and a SPICE kernel.

    The metakernel uses plain relative paths (``spice/ck/...``) as produced
    by ``generate_spice_metakernel``, i.e. paths relative to the datastore root.
    """
    datastore = base / "source_datastore"

    # Place a kernel at spice/ck/imap_dps_test.ah.bc
    kernel_dir = datastore / "spice" / "ck"
    kernel_dir.mkdir(parents=True)
    kernel_file = kernel_dir / "imap_dps_test.ah.bc"
    kernel_file.write_text("fake kernel")

    # Place a metakernel with a plain relative path (not $KERNELS syntax)
    mk_dir = datastore / "spice" / "mk"
    mk_dir.mkdir(parents=True)
    metakernel = mk_dir / "imap_mag_metakernel_test.tm"
    metakernel.write_text(
        "\\begintext\n"
        "test metakernel\n"
        "\\begindata\n"
        "KERNELS_TO_LOAD = (\n"
        "  'spice/ck/imap_dps_test.ah.bc'\n"
        ")\n"
    )

    return datastore


def _make_builder(
    datastore: Path, disk_threshold: float = 0.99
) -> SparseDatastoreBuilder:
    return SparseDatastoreBuilder(
        source_datastore=datastore,
        config=SparseDatastoreConfig(patterns=[]),
        disk_usage_threshold=disk_threshold,
    )


class TestCopyMetakernelAndKernels:
    """The sparse datastore correctly mirrors SPICE kernels without duplicating
    the 'spice' directory segment in the path."""

    def test_kernel_is_copied_to_sparse_datastore(self, tmp_path):
        """SPICE kernel referenced by the metakernel is copied to the target root."""
        datastore = _make_source_datastore(tmp_path)
        builder = _make_builder(datastore)
        target_root = tmp_path / "sparse"
        target_root.mkdir()

        builder._copy_metakernel_and_kernels("imap_mag_metakernel_test.tm", target_root)

        expected = target_root / "spice" / "ck" / "imap_dps_test.ah.bc"
        assert expected.exists(), (
            f"Kernel should be copied to {expected}; "
            "double 'spice' in path would put it under spice/spice/ instead."
        )

    def test_no_double_spice_in_destination(self, tmp_path):
        """Kernel must NOT appear under a double-nested spice/spice/ path."""
        datastore = _make_source_datastore(tmp_path)
        builder = _make_builder(datastore)
        target_root = tmp_path / "sparse"
        target_root.mkdir()

        builder._copy_metakernel_and_kernels("imap_mag_metakernel_test.tm", target_root)

        wrong_path = target_root / "spice" / "spice" / "ck" / "imap_dps_test.ah.bc"
        assert not wrong_path.exists(), (
            f"Kernel must not appear at double-spice path {wrong_path}."
        )

    def test_metakernel_is_written_to_sparse_datastore(self, tmp_path):
        """Rewritten metakernel is placed at spice/mk/ in the target root."""
        datastore = _make_source_datastore(tmp_path)
        builder = _make_builder(datastore)
        target_root = tmp_path / "sparse"
        target_root.mkdir()

        builder._copy_metakernel_and_kernels("imap_mag_metakernel_test.tm", target_root)

        dest_mk = target_root / "spice" / "mk" / "imap_mag_metakernel_test.tm"
        assert dest_mk.exists()

    def test_missing_kernel_logs_warning_not_raises(self, tmp_path, caplog):
        """A missing kernel emits a warning rather than raising an exception."""
        datastore = _make_source_datastore(tmp_path)

        # Remove the kernel so it cannot be found
        (datastore / "spice" / "ck" / "imap_dps_test.ah.bc").unlink()

        builder = _make_builder(datastore)
        target_root = tmp_path / "sparse"
        target_root.mkdir()

        import logging

        with caplog.at_level(logging.WARNING):
            builder._copy_metakernel_and_kernels(
                "imap_mag_metakernel_test.tm", target_root
            )

        assert any("not found" in record.message for record in caplog.records), (
            "Expected a 'not found' warning for the missing kernel"
        )
