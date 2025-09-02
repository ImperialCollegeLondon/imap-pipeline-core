import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))

logging.getLogger("imap_db").setLevel(logging.INFO)
