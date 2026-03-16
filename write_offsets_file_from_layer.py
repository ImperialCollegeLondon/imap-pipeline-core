from pathlib import Path

from src.mag_toolkit.calibration import CalibrationLayer

norm_or_burst = "burst"  # "burst" or "norm"
version = str(1).zfill(3)

for day in range(20, 28):
    year = 2025
    month = 12
    day = str(day).zfill(2)

    layer = CalibrationLayer.from_file(
        Path(
            f"test_data_store/calibration/layers/{year}/{month}/imap_mag_manual-{norm_or_burst}-layer_{year}{month}{day}_v008.json"
        ),
        load_contents=True,
    )
    print(layer._contents.head())
    layer.writeToFile(
        Path(
            f"offsets/imap_mag_l2-{norm_or_burst}-offsets_{year}{month}{day}_{year}{month}{day}_v{version}.cdf"
        )
    )
    print(
        f"Written offsets/imap_mag_l2-{norm_or_burst}-offsets_{year}{month}{day}_{year}{month}{day}_v{version}.cdf"
    )
