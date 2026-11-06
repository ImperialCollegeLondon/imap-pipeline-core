from imap_mag import get_version


def get_cdf_data_version(version: int, version_major: int = 0) -> str:
    """Format a file version as a CDF ``Data_version`` string."""

    if version_major > 0:
        return f"{version_major:03d}.{version:04d}"
    return f"{version:03d}"


def get_gse_version_string() -> str:
    return f"MAG imap-pipeline v{get_version()}"
