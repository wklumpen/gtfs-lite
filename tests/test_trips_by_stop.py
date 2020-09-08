import os
import sys
from datetime import date, time, timedelta

from gtfslite import GTFS

def test_trips_by_stop(feed_zipfile, test_date):
    gtfs = GTFS.load_zip(feed_zipfile)
    print(gtfs.trips_at_stops([2319673, 815392], test_date))