from datetime import date, time, timedelta

from gtfslite import GTFS


march = GTFS.load_zip(r"C:\Users\Willem\Documents\Project\GTFS-lite\mbta_benchmark.zip")
may = GTFS.load_zip(r"C:\Users\Willem\Documents\Project\GTFS-lite\mbta_current.zip")

start_date = date(2020, 4, 19)
start_time = time(0, 0)
end_time = time(23, 59)

print(may.summary())

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

old_total = 0
new_total = 0

for d in daterange(date(2020, 3, 9), date(2020, 3, 16)):
    old_total += march.service_hours(d, start_time, end_time)

for d in daterange(date(2020, 5, 11), date(2020, 5, 18)):
    new_total += may.service_hours(d, start_time, end_time)

print(old_total)
print(new_total)
print((new_total-old_total)/old_total)

