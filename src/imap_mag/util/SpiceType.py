from enum import Enum
from typing import TypeAlias

from imap_data_access.file_validation import _SPICE_DIR_MAPPING

_SpiceType = Enum(
    "SPICEType",
    [(k, k) for k in _SPICE_DIR_MAPPING.keys()],
    type=str,
)

# Python static code analyzers do not recognize _SpiceType as an enum,
# thus use a TypeAlias to hide implementation and make them think it's a type.
SpiceType: TypeAlias = _SpiceType  # type: ignore
