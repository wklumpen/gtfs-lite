
# Start by important a main GTFS object
from gtfslite import GTFS
from datetime import date

# You can load directly from a zipfile
feed = GTFS.load_zip("data/bart.zip")

# feed = GTFS.load_zip("metrolinx.zip")

# Print a summary of the data set
print(feed.route_summary(route_id=2, date=date(2022, 10, 11)))