# GTFS-Lite
A lightweight pandas-driven package for analyzing static GTFS feeds.

GTFS-Lite is a simple module that allows for the quick loading and manipulation
of statig GTFS feeds by leveraging the flexibility of the Pandas library.

You can find the docs [here](https://gtfs-lite.readthedocs.io/).

To get started:
* Install this package using `pip install gtfs-lite`.
* Load a feed directly from a zipfile with `from gtfslite import GTFS`  
and `gtfs = GTFS.load_zip('path/to/file.zip')`
* Access the various attributes, for example `print(gtfs.summary())`