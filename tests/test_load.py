from gtfslite import GTFS
import pytest

class TestLoad:
    def test_loading_by_zipfile(self, feed_zipfile):
        gtfs = GTFS.load_zip(feed_zipfile)
        print(gtfs.summary())