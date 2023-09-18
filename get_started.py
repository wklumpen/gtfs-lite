# Start by important a main GTFS object
from gtfslite import GTFS
from datetime import date

import pandas as pd

# You can load directly from a zipfile
feed = GTFS.load_zip("data/ttc_gtfs_2022-09-01.zip")
# feed = GTFS.load_zip("data/bart.zip")
# feed.set_date(date(2022, 10, 11))

# feed = GTFS.load_zip("metrolinx.zip")

# print(feed.stop_times.arrival_time.max())
print(feed.summary())
# Print a summary of the data set
mx = feed.route_frequency_matrix(date(2022, 9, 14), interval=60, start_time="05:00", end_time="12:00")

mx = pd.merge(mx, feed.routes, on="route_id")
mx.to_csv("ttc_frequency_matrix.csv", index=False)
