from enum import Enum

from imap_data_access.file_validation import _SPICE_DIR_MAPPING

SPICEType = Enum(
    "SPICEType",
    [k for k in _SPICE_DIR_MAPPING.keys()],
    type=str,
)
