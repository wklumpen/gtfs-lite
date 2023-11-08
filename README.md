# GTFS-Lite
A lightweight Pandas-driven package for analyzing static GTFS feeds.

GTFS-Lite was created out of a desire to be able to quickly load static GTFS
feeds into memory and ask specific questions about the dataset in the form of
various metrics and manipulation. Examples include:

* **Basic Summaries:** Trip counts, spans, feed validity, distributions of trips
* **Frequency Metrics:** Frequency by time of day, route, or stop
* **Counting Unique Trips at Stops** Trip counts for a subset of stops
* **Comprehensive date validation** that takes into account calendar and calendar dates

You can find the docs [here](https://gtfs-lite.readthedocs.io/).

To get started:
* Install this package using `pip install gtfs-lite`.
* Load a feed directly from a zipfile with `from gtfslite import GTFS`  
and `gtfs = GTFS.load_zip('path/to/file.zip')`
* Access the various attributes, for example `print(gtfs.summary())`