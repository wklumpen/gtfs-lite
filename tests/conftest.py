from datetime import date, time
import pytest

@pytest.fixture
def feed_zipfile():
    return r"data/SRTA GTFS-2020-06-29.zip"

@pytest.fixture
def test_date():
    return date(2020, 7, 2)

@pytest.fixture
def test_timerange():
    return [time(0, 0), time(23, 59)]