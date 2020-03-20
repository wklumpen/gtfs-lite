
# Start by important a main GTFS object
from gtfslite import GTFS

# You can load directly from a zipfile
feed = GTFS.load_zip("calgary.gtfs.zip")
# feed = GTFS.load_zip("metrolinx.zip")

# Print a summary of the data set
print(feed.calendar)