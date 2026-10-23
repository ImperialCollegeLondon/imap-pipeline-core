from contextlib import nullcontext as does_not_raise

import pytest


@pytest.mark.parametrize(
    "name,table,datetime_column,older_than,raises",
    [
        pytest.param(
            "delete-rows-ace-mag-noaa-older-7d",
            "ace_mag_noaa",
            "id",
            "7d",
            does_not_raise(),
            id="Valid config",
        ),
        pytest.param(
            "delete-rows-ace-mag-noaa-older-7d",
            "ace_mag_noaa",
            "id(",
            "7d",
            pytest.raises(ValueError),
            id="Invalid datetime column",
        ),
        pytest.param(
            "delete-rows-ace-mag-noaa-older-7d",
            "ace_mag_(noaa)",
            "id",
            "7d",
            pytest.raises(ValueError),
            id="Invalid table",
        ),
        pytest.param(
            "delete-rows-ace-mag-noaa-older-7d",
            "ace_mag_(noaa)",
            "id",
            "7weeks",
            pytest.raises(ValueError),
            id="Invalid older_than",
        ),
    ],
)
def test_configuration(name, table, datetime_column, older_than, raises):
    from imap_mag.config.DeleteDatabaseRowsConfig import DeleteRowsTask

    with raises:
        DeleteRowsTask(
            name=name,
            table=table,
            datetime_column=datetime_column,
            older_than=older_than,
        )
