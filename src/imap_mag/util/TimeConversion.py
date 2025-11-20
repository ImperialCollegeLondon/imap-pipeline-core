from datetime import date, datetime

import numpy as np
import numpy.typing as npt

from imap_mag.util.constants import CONSTANTS


class TimeConversion:
    @staticmethod
    def convert_met_to_j2000ns(
        met: npt.ArrayLike,
        reference_epoch: np.datetime64 = CONSTANTS.IMAP_EPOCH,
    ) -> npt.ArrayLike:
        """Convert mission elapsed time (MET) to nanoseconds from J2000."""
        time_array = (np.asarray(met, dtype=float) * 1e9).astype(np.int64)
        j2000_offset = (
            (reference_epoch - CONSTANTS.J2000_EPOCH)
            .astype("timedelta64[ns]")
            .astype(np.int64)
        )
        return j2000_offset + time_array

    @staticmethod
    def convert_j2000ns_to_datetime(
        j2000ns: npt.ArrayLike,
    ) -> list[datetime]:
        """Convert nanoseconds from J2000 to Python datetime."""
        return [
            datetime.fromtimestamp(j)
            for j in (
                np.asarray(j2000ns, dtype=float).astype(np.int64) / 1e9
                + CONSTANTS.J2000_EPOCH_POSIX
            )
        ]

    @staticmethod
    def convert_j2000ns_to_isostring(
        j2000ns: npt.ArrayLike,
    ) -> list[str]:
        """Convert nanoseconds from J2000 to ISO 8601 string."""
        return [
            datetime.fromtimestamp(j).isoformat()
            for j in (
                np.asarray(j2000ns, dtype=float).astype(np.int64) / 1e9
                + CONSTANTS.J2000_EPOCH_POSIX
            )
        ]

    @staticmethod
    def convert_j2000ns_to_date(
        j2000ns: npt.ArrayLike,
    ) -> list[date]:
        """Convert nanoseconds from J2000 to Python date."""
        return [
            date.fromtimestamp(j)
            for j in (
                np.asarray(j2000ns, dtype=float).astype(np.int64) / 1e9
                + CONSTANTS.J2000_EPOCH_POSIX
            )
        ]

    @staticmethod
    def convert_met_to_date(
        met: npt.ArrayLike,
        reference_epoch: np.datetime64 = CONSTANTS.IMAP_EPOCH,
    ) -> list[date]:
        """
        Convert mission elapsed time (MET) to Python date.

        Note that this does not use SPICE, thus it may differ slightly from SDC-decoded times.
        This function should NOT be used for science decoding!
        """
        j2000ns = TimeConversion.convert_met_to_j2000ns(met, reference_epoch)
        return TimeConversion.convert_j2000ns_to_date(j2000ns)

    @staticmethod
    def try_extract_iso_like_datetime(dict, key) -> datetime | None:
        # Handle both "YYYY-MM-DD, HH:MM:SS" and "YYYY-MM-DD HH:MM:SS" formats stored in a dictionary

        if not dict or not key or key not in dict:
            return None

        result = dict.get(key)
        result = result.replace(", ", " ")
        try:
            result_date = datetime.fromisoformat(result)
            return result_date
        except ValueError:
            return None
