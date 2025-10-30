import logging
from dataclasses import dataclass

from imap_mag.io.file.QuicklookPathHandler import QuicklookPathHandler

logger = logging.getLogger(__name__)


@dataclass
class IALiRTQuicklookPathHandler(QuicklookPathHandler):
    """
    Path handler for I-ALiRT figures.
    """

    @staticmethod
    def get_plot_type() -> str:
        return "ialirt"
