.. gtfs-lite documentation master file, created by
   sphinx-quickstart on Tue Sep  8 17:25:23 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

GTFS Lite
=======================

GTFS Lite was born out of a need for a lightweight, easy-to-get-going way to 
quickly load, read, and analyze static GTFS feeds using Python 3.

Quick Start
-----------

You can load a zipped GTFS feed into memory using the :py:meth:`load_zip` 
function::

   from gtfslite.gtfs import GTFS
   gtfs = GTFS.load_zip('path/to/file.zip')

From there, a number of helper and analytical methods are accessible.

.. note::
   This project is still under development, and new features and releases are
   being added regularly. Be sure to install the latest version and update
   frequently.


.. toctree::
   :maxdepth: 2
   :caption: Table of Contents

   gtfs



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
