from datetime import date, time
import pytest

@pytest.fixture
def feed_zipfile():
    return r"data/bart.zip"

@pytest.fixture
def test_date():
    return date(2022, 10, 2)

@pytest.fixture
def test_date_invalid():
    return date(2020, 10, 2)

@pytest.fixture
def test_timerange():
    return [time(0, 0), time(23, 59)]

@pytest.fixture
def test_stop_ids():
    return [time(0, 0), time(23, 59)]