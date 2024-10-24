from pathlib import Path

from cdflib import xarray


def load_cdf(inputPath: Path):
    """Wraps cdlibs xarray reader."""
    if inputPath.is_file():
        return xarray.cdf_to_xarray(inputPath)
    else:
        raise FileNotFoundError()


def write_cdf(dataset, outputPath: Path):
    """Wraps cdflib xarray writer."""
    xarray.xarray_to_cdf(dataset, outputPath)
