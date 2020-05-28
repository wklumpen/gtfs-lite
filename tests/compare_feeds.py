from gtfslite import GTFS


# Load in first feed
april = GTFS.load_zip(r"C:\Users\Willem\Documents\Project\GTFS-lite\ttc_april.zip")
may = GTFS.load_zip(r"C:\Users\Willem\Documents\Project\GTFS-lite\ttc_may.zip")


april.compare(may)
