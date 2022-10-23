from gtfslite import GTFS

def test_set_date(feed_zipfile, test_date):
    gtfs = GTFS.load_zip(feed_zipfile)
    gtfs.set_date(test_date)