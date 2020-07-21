from datetime import date, time, timedelta
from gtfslite import GTFS

def test_service_hours(feed_zipfile):
    pass



# april = GTFS.load_zip(r"tests\data\ttc_april.zip")
# may = GTFS.load_zip(r"tests\data\ttc_may.zip")

# start_date = date(2020, 4, 19)
# start_time = time(0, 0)
# end_time = time(23, 59)

# print("Testing service hour calculation and comparison")

# print(april.summary())


# print(may.summary())

# def daterange(start_date, end_date):
#     for n in range(int ((end_date - start_date).days)):
#         yield start_date + timedelta(n)

# old_total = 0
# new_total = 0

# for d in daterange(date(2020, 4, 20), date(2020, 4, 28)):
#     old_total += april.service_hours(d, start_time, end_time)

# for d in daterange(date(2020, 5, 11), date(2020, 5, 18)):
#     new_total += may.service_hours(d, start_time, end_time)

# print(old_total)
# print(new_total)
# print((new_total-old_total)/old_total)

