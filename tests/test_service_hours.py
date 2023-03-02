from gtfslite import GTFS, DateNotValidException
import pytest


def test_service_hours(feed_zipfile, test_date, test_timerange):
    gtfs = GTFS.load_zip(feed_zipfile)
    # TODO: Do a manual check with a simpler feed
    gtfs.service_hours(test_date, "10:00:00", "12:00:00")


def test_service_hours_invalid_date(feed_zipfile, test_date_invalid, test_timerange):
    gtfs = GTFS.load_zip(feed_zipfile)
    with pytest.raises(DateNotValidException):
        gtfs.service_hours(test_date_invalid, test_timerange[0], test_timerange[1])
