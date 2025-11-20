from pathlib import Path

from src.mag_toolkit.calibration import CalibrationLayer

for day in range(1, 16):
    year = 2025
    month = 10
    day = str(day).zfill(2)
    version = str(1).zfill(3)
    layer = CalibrationLayer.from_file(
        Path(
            f"test_data_store/calibration/layers/{year}/{month}/imap_mag_manual-norm-layer_{year}{month}{day}_v002.json"
        ),
        load_contents=True,
    )
    print(layer._contents.head())
    layer.writeToFile(
        Path(
            f"offsets/imap_mag_l2-norm-offsets_{year}{month}{day}_{year}{month}{day}_v{version}.cdf"
        )
    )
    print(
        f"Written offsets/imap_mag_l2-norm-offsets_{year}{month}{day}_{year}{month}{day}_v{version}.cdf"
    )
