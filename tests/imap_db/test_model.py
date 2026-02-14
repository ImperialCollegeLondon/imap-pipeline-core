from imap_db.model import File


def test_get_descriptor_from_filename():
    test_cases = [
        ("simplefile.txt", "simplefile"),
        ("complex_name_with_underscores_v001.docx", "complex_name_with_underscores"),
        ("imap_mag_l1_hsk-status_20251201_v001.csv", "imap_mag_l1_hsk-status"),
        ("imap_mag_l1_hsk-status_20251201_001.csv", "imap_mag_l1_hsk-status"),
        ("imap_ialirt_20251201.csv", "imap_ialirt"),
        (
            "imap_mag_l2-burst-offsets_20250421_20250421_v000.cdf",
            "imap_mag_l2-burst-offsets",
        ),
        ("imap_mag_l1d_burst-srf_20251207_v001.cdf", "imap_mag_l1d_burst-srf"),
        ("report_2023-05-01_v10.pdf", "report"),
    ]

    for filename, expected_descriptor in test_cases:
        descriptor = File.get_descriptor_from_filename(filename)
        assert descriptor == expected_descriptor, f"Failed for filename: {filename}"
