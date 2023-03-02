import datetime

import pytest

from gtfslite import GTFS


@pytest.mark.parametrize(
    "test_date, expected",
    [(datetime.date(2022, 10, 1), True), (datetime.date(2020, 12, 2), False)],
)
def test_valid_date(feed_zipfile, test_date, expected):
    gtfs = GTFS.load_zip(feed_zipfile)
    assert gtfs.valid_date(test_date) == expected
