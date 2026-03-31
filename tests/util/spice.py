import shutil
import urllib.request
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from imap_db.model import File
from imap_mag.cli.fetch.spice import MAXIMUM_J2000_INTERVAL
from imap_mag.io.file import SPICEPathHandler
from tests.util.miscellaneous import DATASTORE

DE440_URL = "https://naif.jpl.nasa.gov/pub/naif/generic_kernels/spk/planets/de440.bsp"
DE440_SOURCE_PATH = DATASTORE / "spice" / "spk" / "de440.bsp"


@pytest.fixture(autouse=False, scope="function")
def spice_kernels(temp_datastore):
    """Mock the Database to return File records for test SPICE kernels.

    Downloads de440.bsp from NASA NAIF to tests/datastore/spice/spk/ if not
    already present (the file is gitignored due to its size), then scans
    temp_datastore/spice/ and creates mock File records for all recognised
    kernel files so that generate_spice_metakernel() succeeds.
    """
    # Download de440.bsp to the canonical source location if absent
    if not DE440_SOURCE_PATH.exists():
        DE440_SOURCE_PATH.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(DE440_URL, DE440_SOURCE_PATH)

    # temp_datastore is a copy of tests/datastore/ made at fixture setup time.
    # If de440.bsp was just downloaded it won't be in the temp copy — add it.
    temp_de440 = temp_datastore / "spice" / "spk" / "de440.bsp"
    if not temp_de440.exists():
        temp_de440.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(DE440_SOURCE_PATH, temp_de440)

    # Scan temp_datastore/spice/ for all kernel files, excluding the mk/ subfolder
    spice_root = temp_datastore / "spice"
    mock_files = []
    for kernel_file in sorted(spice_root.rglob("*")):
        if not kernel_file.is_file():
            continue
        rel = kernel_file.relative_to(spice_root)
        if "mk" in rel.parts:
            continue  # skip metakernel folder

        kernel_type = SPICEPathHandler.get_kernel_type_from_filename(kernel_file.name)
        if kernel_type is None:
            continue  # skip unrecognised files (e.g. spin files)

        mock_files.append(
            File(
                path=kernel_file.relative_to(temp_datastore).as_posix(),
                file_meta={
                    "kernel_type": kernel_type,
                    "version": "1",
                    "file_intervals_j2000": MAXIMUM_J2000_INTERVAL,
                    "timestamp": datetime.now(UTC).timestamp(),
                },
                last_modified_date=datetime.now(UTC),
            )
        )

    mock_db = MagicMock()
    mock_db.get_files_by_path.return_value = mock_files

    with patch("imap_mag.cli.fetch.spice.Database") as mock_database_class:
        mock_database_class.return_value = mock_db
        yield mock_files
