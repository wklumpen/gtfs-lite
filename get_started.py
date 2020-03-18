from gtfslite import GTFS

feed = GTFS.load_zip("calgary.gtfs.zip")
print(feed.stops)