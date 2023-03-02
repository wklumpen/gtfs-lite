
# Start by important a main GTFS object
from gtfslite import GTFS
from datetime import date

import pandas as pd

# You can load directly from a zipfile
feed = GTFS.load_zip("data/yyc_gtfs_2022-09-28.zip")
# feed.set_date(date(2022, 10, 11))

# feed = GTFS.load_zip("metrolinx.zip")

# Print a summary of the data set
mx = feed.route_frequency_matrix(date(2022, 10, 12), interval=30)
mx = pd.merge(mx, feed.routes, on='route_id')
mx.to_csv('calgary_frequency_matrix.csv', index=False)