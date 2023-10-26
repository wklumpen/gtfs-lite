.. gtfs-lite documentation master file, created by
   sphinx-quickstart on Tue Sep  8 17:25:23 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

GTFS Lite
=======================

GTFS Lite is a lightweight, Pandas-driven approach to loading, analyzing, and
manipulating transit schedule data formatted following the `General Transit Feed
Specification <https://gtfs.org/schedule/reference/>`_.

The design of GTFS Lite is focused primarily on being able to load and quickly
manipulate or study GTFS feeds. It is not intended as a high-performance GTFS
editor. This package was initially developed out of a need to support academic
and data science projects, and for basic data wrangling needs for larger
analyses.

Quick Start
-----------

You can load a zipped GTFS feed into memory using the :py:meth:`load_zip` 
function::

   from gtfslite import GTFS
   gtfs = GTFS.load_zip('path/to/file.zip')

From there, a number of analytical methods are available. For example, you can
compute get an at-a-glance summary of the dataset with::

   gtfs.summary()

Which provdes a :py:class:`pandas.Series`` object containing various pieces of
information about the dataset including total stops, trips, routes, and date
coverage.

You can also manipuate GTFS files directly using the associated dataframes. For
example::

   gtfs.calendar["start_date"] = "20200220"

would set the calendar start date on all services to February 20, 2020. If you
would like to write a modified GTFS object to a zipfile for future use, you can
do so with::

   gtfs.write_zip("path/to/outfile.zip")

.. note::
   This project is still under development, and new features and releases are
   being added regularly. Be sure to install the latest version and update
   frequently.


.. toctree::
   :maxdepth: 2
   :caption: Table of Contents

   contributing
   gtfs



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
