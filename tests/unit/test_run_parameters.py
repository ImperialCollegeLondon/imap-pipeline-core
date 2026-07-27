"""Tests for RunParameters schema field ordering.

The Prefect UI form renders fields in `position` order (falling back to 0
when absent). start_date/end_date previously had no position hint, which is
believed to have caused the UI to render end_date before start_date -
confusing users filling in a date range. This pins the intended order.
"""

from pydantic import TypeAdapter

from imap_mag.data_pipelines.RunParameters import FetchByDatesRunParameters


def test_fetch_by_dates_run_parameters_orders_start_before_end():
    schema = TypeAdapter(FetchByDatesRunParameters).json_schema()
    properties = schema["properties"]

    assert properties["start_date"]["position"] < properties["end_date"]["position"]
    assert (
        properties["end_date"]["position"] < properties["force_redownload"]["position"]
    )
