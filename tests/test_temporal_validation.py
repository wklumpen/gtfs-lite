import os
import sys
from datetime import date, time, timedelta

from gtfslite import GTFS

folder = r"tests\data\feeds_2020-02-29"
start_date = date(2020, 3, 2)
end_date = date(2020, 3, 8)

for subdir, dirs, files in os.walk(folder):
    for file in files:
        print(file)
        g = GTFS.load_zip(os.path.join(subdir, file))
        if g.trip_distribution(start_date, end_date)['monday'] < 1:
            print('NOPE')
        else:
            print('YEP')
