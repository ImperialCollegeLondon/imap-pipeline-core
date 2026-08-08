from datetime import date, datetime
from unittest.mock import patch

from imap_mag.config import SaveMode
from imap_mag.util import ScienceMode
from mag_toolkit.calibration.CalibrationConfig import GradiometryConfig
from prefect_server.performCalibration import (
    apply_flow,
    calibrate_and_apply_flow,
)
from tests.util.miscellaneous import open_cdf
from tests.util.prefect_test_utils import prefect_test_fixture  # noqa: F401


def test_apply_flow_resolves_layer_patterns_and_discovers_science_file(
    temp_datastore,
    dynamic_work_folder,
    spice_kernels,
    prefect_test_fixture,  # noqa: F811
):
    apply_flow(
        layers=["*noop*"],
        start_date=datetime(2026, 1, 16),
        mode=ScienceMode.Normal,
        save_mode=SaveMode.LocalOnly,
    )

    date_ = datetime(2026, 1, 16)
    output_l2_file = (
        temp_datastore
        / f"science/mag/l2-pre/{date_.year}/{date_.month:02d}/imap_mag_l2-pre_norm-srf_{date_.year}{date_.month:02d}{date_.day:02d}_v001.0001.cdf"
    )
    assert output_l2_file.exists()

    output_offsets_file = (
        temp_datastore
        / f"science-ancillary/l2-offsets/{date_.year}/{date_.month:02d}/imap_mag_l2-norm-offsets_{date_.year}{date_.month:02d}{date_.day:02d}_{date_.year}{date_.month:02d}{date_.day:02d}_v001.cdf"
    )
    assert output_offsets_file.exists()

    with open_cdf(output_l2_file) as cdf:
        assert "b_srf" in cdf
        assert "epoch" in cdf
        assert "magnitude" in cdf


def test_apply_flow_offset_version_override(
    temp_datastore,
    dynamic_work_folder,
    spice_kernels,
    prefect_test_fixture,  # noqa: F811
):
    """apply_flow must honour offset_version_override and write the offset file
    at the forced version, overwriting any existing placeholder at that slot."""
    offsets_dir = temp_datastore / "science-ancillary/l2-offsets/2026/01"
    offsets_dir.mkdir(parents=True, exist_ok=True)
    existing = offsets_dir / "imap_mag_l2-norm-offsets_20260116_20260116_v007.cdf"
    existing.write_bytes(b"old content")

    apply_flow(
        layers=["*noop*"],
        start_date=datetime(2026, 1, 16),
        mode=ScienceMode.Normal,
        save_mode=SaveMode.LocalOnly,
        offset_version_override=7,
    )

    assert existing.exists()
    assert existing.stat().st_size > len(b"old content"), (
        "Offset file must have been overwritten with real content at v007"
    )
    assert not (
        offsets_dir / "imap_mag_l2-norm-offsets_20260116_20260116_v008.cdf"
    ).exists(), "No auto-increment to v008 when override is active"


def test_apply_flow_l2_version_override(
    temp_datastore,
    dynamic_work_folder,
    spice_kernels,
    prefect_test_fixture,  # noqa: F811
):
    """apply_flow must honour l2_version_override and produce L2-pre science files
    at the forced (major, minor) version."""
    apply_flow(
        layers=["*noop*"],
        start_date=datetime(2026, 1, 16),
        mode=ScienceMode.Normal,
        save_mode=SaveMode.LocalOnly,
        l2_version_override=(3, 9),
    )

    l2_dir = temp_datastore / "science/mag/l2-pre/2026/01"
    forced = l2_dir / "imap_mag_l2-pre_norm-srf_20260116_v003.0009.cdf"
    assert forced.exists(), "L2-pre SRF file must exist at forced version v003.0009"
    assert not (l2_dir / "imap_mag_l2-pre_norm-srf_20260116_v001.0001.cdf").exists(), (
        "Default v001.0001 must not appear when L2 version override is active"
    )


def test_calibrate_and_apply_flow_with_version_overrides(
    temp_datastore,
    dynamic_work_folder,
    spice_kernels,
):
    """calibrate_and_apply_flow must forward offset_version_override and
    l2_version_override to the apply step, producing output files at the
    forced versions.

    The calibration step is mocked to return the real noop layer that already
    exists in the test datastore so the apply step runs end-to-end.
    """
    layer_path = (
        temp_datastore
        / "calibration/layers/2026/01/imap_mag_noop-norm-layer_20260116_v001.json"
    )

    with patch(
        "prefect_server.performCalibration._run_calibration",
        return_value=[layer_path],
    ):
        calibrate_and_apply_flow.fn(
            start_date=date(2026, 1, 16),
            configuration=GradiometryConfig(kappa=0.0, sc_interference_threshold=0.0),
            mode=ScienceMode.Normal,
            save_mode=SaveMode.LocalOnly,
            offset_version_override=4,
            l2_version_override=(2, 8),
        )

    offsets_dir = temp_datastore / "science-ancillary/l2-offsets/2026/01"
    assert (
        offsets_dir / "imap_mag_l2-norm-offsets_20260116_20260116_v004.cdf"
    ).exists(), "Offset file must be at forced version v004"

    l2_dir = temp_datastore / "science/mag/l2-pre/2026/01"
    assert (l2_dir / "imap_mag_l2-pre_norm-srf_20260116_v002.0008.cdf").exists(), (
        "L2-pre SRF file must be at forced version v002.0008"
    )
