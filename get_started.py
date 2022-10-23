
# Start by important a main GTFS object
from gtfslite import GTFS
from datetime import date

# You can load directly from a zipfile
feed = GTFS.load_zip("data/mbta_current.zip")
# feed.set_date(date(2022, 10, 11))

# feed = GTFS.load_zip("metrolinx.zip")

# Print a summary of the data set
print(feed.stop_times_at_stop(70061, date(2020, 5, 13), start_time='07:00:00'))