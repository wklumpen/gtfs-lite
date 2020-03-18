
# Start by important a main GTFS object
from gtfslite import GTFS

# You can load directly from a zipfile
feed = GTFS.load_zip("calgary.gtfs.zip")
print(feed.stops)