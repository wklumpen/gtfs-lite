from gtfslite import GTFS

gtfs = GTFS.load_zip("tests/data/ttc_april.zip")
print(gtfs.summary())