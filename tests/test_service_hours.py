from datetime import date, time, timedelta
from gtfslite import GTFS

def test_service_hours(feed_zipfile, test_date, test_timerange):
    gtfs = GTFS.load_zip(feed_zipfile)
    return gtfs.service_hours(test_date, test_timerange[0], test_timerange[1])

