"""Tests for CCSDSBinaryPacketFile."""

from datetime import date
from pathlib import Path

from imap_mag.util.CCSDSBinaryPacketFile import CCSDSBinaryPacketFile

TEST_DATA = Path(__file__).parent.parent / "test_data"
HK_PW_PKTS = TEST_DATA / "MAG_HSK_PW.pkts"


class TestGetApids:
    def test_returns_set_of_apids_from_pkts_file(self):
        result = CCSDSBinaryPacketFile(HK_PW_PKTS).get_apids()
        assert isinstance(result, set)
        assert len(result) > 0

    def test_known_apid_is_present(self):
        result = CCSDSBinaryPacketFile(HK_PW_PKTS).get_apids()
        assert 1063 in result


class TestGetDaysByApid:
    def test_returns_dict_with_apid_keys_and_date_sets(self):
        result = CCSDSBinaryPacketFile(HK_PW_PKTS).get_days_by_apid()
        assert isinstance(result, dict)
        assert len(result) > 0
        for apid, days in result.items():
            assert isinstance(apid, int)
            assert isinstance(days, set)
            for d in days:
                assert isinstance(d, date)

    def test_known_apid_maps_to_expected_date(self):
        result = CCSDSBinaryPacketFile(HK_PW_PKTS).get_days_by_apid()
        assert 1063 in result
        assert date(2025, 5, 2) in result[1063]

    def test_returns_empty_dict_when_another_valid_pkts_file_is_used(self):
        pkts_file = TEST_DATA / "imap_sc_l0_x286_20260204_001.pkts"
        result = CCSDSBinaryPacketFile(pkts_file).get_days_by_apid()
        assert isinstance(result, dict)


class TestSplitPacketsByDay:
    def test_returns_dict_with_date_keys_and_bytearray_values(self):
        result = CCSDSBinaryPacketFile(HK_PW_PKTS).split_packets_by_day()
        assert isinstance(result, dict)
        for day, data in result.items():
            assert isinstance(day, date)
            assert isinstance(data, bytearray)

    def test_known_date_is_in_result(self):
        result = CCSDSBinaryPacketFile(HK_PW_PKTS).split_packets_by_day()
        assert date(2025, 5, 2) in result

    def test_split_data_is_non_empty(self):
        result = CCSDSBinaryPacketFile(HK_PW_PKTS).split_packets_by_day()
        for day, data in result.items():
            assert len(data) > 0


class TestCombineDaysByApid:
    def test_combines_days_from_multiple_files(self):
        files = [HK_PW_PKTS, TEST_DATA / "MAG_HSK_SCI.pkts"]
        result = CCSDSBinaryPacketFile.combine_days_by_apid(files)
        assert isinstance(result, dict)
        assert len(result) >= 1

    def test_single_file_gives_same_as_direct_call(self):
        direct = CCSDSBinaryPacketFile(HK_PW_PKTS).get_days_by_apid()
        combined = CCSDSBinaryPacketFile.combine_days_by_apid([HK_PW_PKTS])
        assert set(combined.keys()) == set(direct.keys())
        for apid in direct:
            assert combined[apid] == direct[apid]

    def test_combines_days_from_same_file_twice(self):
        result = CCSDSBinaryPacketFile.combine_days_by_apid([HK_PW_PKTS, HK_PW_PKTS])
        direct = CCSDSBinaryPacketFile(HK_PW_PKTS).get_days_by_apid()
        assert set(result.keys()) == set(direct.keys())
