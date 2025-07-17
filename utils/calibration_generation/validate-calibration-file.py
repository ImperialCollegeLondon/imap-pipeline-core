from spacepy import pycdf

"""
Verify that offsets and frame transforms in the CDF file are of the correct shape.
Offsets should be (2, 4, 3) and frame transforms should be (3, 3, 4).
"""

filename = "imap_mag_ialirt-calibration_20250101_v002.cdf"

with pycdf.CDF(filename, readonly=True) as cdf:
    offsets = cdf["offsets"][:]
    mago_transform = cdf["URFTOORFO"][:]
    magi_transform = cdf["URFTOORFI"][:]
    if offsets is None:
        raise ValueError("Offsets data is empty in the CDF file.")
    assert offsets.shape == (
        2,
        4,
        3,
    ), "Offsets data shape is incorrect, expected (2, 4, 3)"
    if mago_transform is None or magi_transform is None:
        raise ValueError("Frame transform data is empty in the CDF file.")
    assert mago_transform.shape == (
        3,
        3,
        4,
    ), "MAGo frame transform shape is incorrect, expected (3, 3, 4)"
    assert magi_transform.shape == (
        3,
        3,
        4,
    ), "MAGi frame transform shape is incorrect, expected (3, 3, 4)"
    print(
        f"Validation of {filename} successful. Offsets and frame transforms are the correct shape."
    )
