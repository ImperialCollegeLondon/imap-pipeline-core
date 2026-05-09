"""Tests for PartitionedPathHandler and HKBinaryPathHandler."""

from datetime import datetime

import pytest

from imap_mag.io.file.HKBinaryPathHandler import HKBinaryPathHandler
from imap_mag.io.file.PartitionedPathHandler import PartitionedPathHandler


def _make_hk_handler():
    return HKBinaryPathHandler(
        descriptor="hsk-pw",
        content_date=datetime(2025, 1, 1),
        extension="pkts",
    )


class TestPartitionedPathHandler:
    def test_get_sequence_returns_part(self):
        handler = _make_hk_handler()
        handler.part = 3
        assert handler.get_sequence() == 3

    def test_set_sequence_updates_part(self):
        handler = _make_hk_handler()
        handler.set_sequence(5)
        assert handler.part == 5

    def test_increase_sequence_increments_part(self):
        handler = _make_hk_handler()
        handler.part = 2
        handler.increase_sequence()
        assert handler.part == 3

    def test_get_sequence_variable_name_is_part(self):
        assert PartitionedPathHandler.get_sequence_variable_name() == "part"


class TestSequenceablePathHandlerAddMetadata:
    def test_add_metadata_raises_not_implemented_error(self):
        handler = _make_hk_handler()
        with pytest.raises(NotImplementedError):
            handler.add_metadata({"key": "value"})

    def test_get_metadata_returns_none_by_default(self):
        handler = _make_hk_handler()
        assert handler.get_metadata() is None
